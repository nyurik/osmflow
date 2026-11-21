//! TLS for database connections, as libpq's `sslmode` and `sslrootcert` in the connection string ask, with rustls.
//!
//! The `postgres` crate knows only `disable`, `prefer` and `require`, does not check certificates itself, and rejects
//! parameters it does not know, so both are taken out of the connection string before it parses the rest.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use percent_encoding::percent_decode_str;
use postgres::config::SslMode as PgSslMode;
use postgres::{Client, Config};
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{
    WebPkiSupportedAlgorithms, ring, verify_tls12_signature, verify_tls13_signature,
};
use rustls::pki_types::pem::PemObject as _;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme,
};
use tokio_postgres_rustls::MakeRustlsConnect;

use crate::SyncError;

/// The TLS settings of a connection string cannot be used.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum TlsError {
    /// `sslmode` is none of those supported; libpq's `allow` is not.
    #[error("sslmode={0} is not supported: use disable, prefer, require, verify-ca or verify-full")]
    SslMode(String),
    /// `sslmode=verify-ca` without `sslrootcert`: `verify-ca` checks no host name, so the system's authorities, which
    /// issue certificates for any host, would accept any server.
    #[error(
        "sslmode=verify-ca needs sslrootcert, a PEM file of the certificate authorities to trust"
    )]
    NoRootCert,
    /// The `sslrootcert` file cannot be read, holds no PEM certificate, or holds one rustls cannot use.
    #[error("sslrootcert {path}: {reason}", path = .path.display())]
    RootCert {
        /// The file.
        path: PathBuf,
        /// What is wrong with it.
        reason: String,
    },
    /// `sslmode=verify-full` without `sslrootcert`, and the system's trusted certificates cannot be loaded: give the
    /// server's certificate authorities in `sslrootcert`.
    #[error(
        "no system certificate authorities to verify the server with; give them in sslrootcert: {0}"
    )]
    SystemRoots(String),
}

/// What to check of the server, from libpq's `sslmode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SslMode {
    /// No TLS.
    Disable,
    /// TLS if the server has it, without checking its certificate; libpq's default.
    Prefer,
    /// TLS, without checking the certificate, unless `sslrootcert` is given: then as `VerifyCa`.
    Require,
    /// TLS with a certificate issued by a trusted authority, for any host name.
    VerifyCa,
    /// TLS with a certificate issued by a trusted authority, for the host connected to.
    VerifyFull,
}

impl SslMode {
    fn parse(value: &str) -> Result<Self, TlsError> {
        Ok(match value {
            "disable" => Self::Disable,
            "prefer" => Self::Prefer,
            "require" => Self::Require,
            "verify-ca" => Self::VerifyCa,
            "verify-full" => Self::VerifyFull,
            _ => return Err(TlsError::SslMode(value.to_owned())),
        })
    }
}

/// Connects as `url` asks, `sslmode` and `sslrootcert` included.
pub(crate) fn connect(url: &str) -> Result<Client, SyncError> {
    let (config, mode, root_cert) = parse(url)?;
    // With `disable`, `postgres` does not use it
    let tls = MakeRustlsConnect::new(client_config(mode, root_cert.as_deref())?);
    Ok(config.connect(tls)?)
}

/// The connection string without `sslmode` and `sslrootcert`, parsed, with the mode `postgres` is to use, and both.
fn parse(url: &str) -> Result<(Config, SslMode, Option<PathBuf>), SyncError> {
    let mut mode = None;
    let mut root_cert = None;
    let rest = split(url, |key, value| match key {
        "sslmode" => {
            mode = Some(SslMode::parse(&value)?);
            Ok(true)
        }
        "sslrootcert" => {
            root_cert = Some(PathBuf::from(value));
            Ok(true)
        }
        _ => Ok(false),
    })?;
    let mode = mode.unwrap_or(SslMode::Prefer);
    let mut config: Config = rest.parse()?;
    config.ssl_mode(match mode {
        SslMode::Disable => PgSslMode::Disable,
        SslMode::Prefer => PgSslMode::Prefer,
        SslMode::Require | SslMode::VerifyCa | SslMode::VerifyFull => PgSslMode::Require,
    });
    Ok((config, mode, root_cert))
}

/// The connection string without the parameters `take` takes: it gets each key and decoded value, and returns whether
/// to take it out.
///
/// A URL's parameters are in its query, `?key=value&…`, percent-encoded. Otherwise they are `key=value` pairs apart by
/// spaces, the value in `'` if it has spaces, and `\` escaping the next character, as `postgres` reads them.
fn split(
    url: &str,
    mut take: impl FnMut(&str, String) -> Result<bool, TlsError>,
) -> Result<String, TlsError> {
    let mut kept = Vec::new();
    if url.starts_with("postgres://") || url.starts_with("postgresql://") {
        let Some((base, query)) = url.split_once('?') else {
            return Ok(url.to_owned());
        };
        for pair in query.split('&') {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let value = percent_decode_str(value).decode_utf8_lossy().into_owned();
            if !take(key, value)? {
                kept.push(pair);
            }
        }
        return Ok(if kept.is_empty() {
            base.to_owned()
        } else {
            format!("{base}?{}", kept.join("&"))
        });
    }
    let mut s = url.trim_start();
    while !s.is_empty() {
        let key_len = s
            .find(|c: char| c == '=' || c.is_whitespace())
            .unwrap_or(s.len());
        let Some(value) = s[key_len..].trim_start().strip_prefix('=') else {
            // Not `key=value`: left for `postgres` to report
            kept.push(s);
            break;
        };
        let value = value.trim_start();
        let Some((decoded, value_len)) = unquote(value) else {
            // A quote without its end: left for `postgres` to report
            kept.push(s);
            break;
        };
        let len = s.len() - value.len() + value_len;
        if !take(&s[..key_len], decoded)? {
            kept.push(&s[..len]);
        }
        s = s[len..].trim_start();
    }
    Ok(kept.join(" "))
}

/// A `key=value` value, in `'` or up to a space, with `\` escaping the next character, and its length in `s`; none if
/// the quote does not end.
fn unquote(s: &str) -> Option<(String, usize)> {
    let quoted = s.starts_with('\'');
    let mut value = String::new();
    let mut chars = s.char_indices().skip(usize::from(quoted));
    while let Some((i, c)) = chars.next() {
        match c {
            '\\' => value.extend(chars.next().map(|(_, c)| c)),
            '\'' if quoted => return Some((value, i + 1)),
            c if !quoted && c.is_whitespace() => return Some((value, i)),
            c => value.push(c),
        }
    }
    (!quoted).then_some((value, s.len()))
}

/// The rustls configuration for `mode`, trusting the authorities in `root_cert`, or the system's.
fn client_config(mode: SslMode, root_cert: Option<&Path>) -> Result<ClientConfig, TlsError> {
    let provider = Arc::new(ring::default_provider());
    let builder = ClientConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .expect("ring has TLS 1.2 and 1.3");
    // As libpq: `require` checks the issuer when `sslrootcert` is given, and `prefer` never checks
    let check_name = match (mode, root_cert) {
        (SslMode::Disable | SslMode::Prefer, _) | (SslMode::Require, None) => {
            let verifier = AnyCertificate(provider.signature_verification_algorithms);
            return Ok(builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(verifier))
                .with_no_client_auth());
        }
        (SslMode::VerifyCa, None) => return Err(TlsError::NoRootCert),
        (SslMode::Require | SslMode::VerifyCa, Some(_)) => false,
        (SslMode::VerifyFull, _) => true,
    };
    let roots = Arc::new(match root_cert {
        Some(path) => file_roots(path)?,
        None => system_roots()?,
    });
    let verifier = WebPkiServerVerifier::builder_with_provider(roots, provider)
        .build()
        .expect("the roots are not empty");
    let verifier: Arc<dyn ServerCertVerifier> = if check_name {
        verifier
    } else {
        Arc::new(AnyName(verifier))
    };
    Ok(builder
        .dangerous()
        .with_custom_certificate_verifier(verifier)
        .with_no_client_auth())
}

/// The certificates in the PEM file at `path`.
fn file_roots(path: &Path) -> Result<RootCertStore, TlsError> {
    let error = |reason: String| TlsError::RootCert {
        path: path.to_path_buf(),
        reason,
    };
    let mut roots = RootCertStore::empty();
    for cert in CertificateDer::pem_file_iter(path).map_err(|e| error(e.to_string()))? {
        let cert = cert.map_err(|e| error(e.to_string()))?;
        roots.add(cert).map_err(|e| error(e.to_string()))?;
    }
    if roots.is_empty() {
        return Err(error("no PEM certificate in it".to_owned()));
    }
    Ok(roots)
}

/// The system's trusted certificates, skipping those it cannot read.
fn system_roots() -> Result<RootCertStore, TlsError> {
    let found = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    let (_added, _skipped) = roots.add_parsable_certificates(found.certs);
    if roots.is_empty() {
        let errors: Vec<_> = found.errors.iter().map(ToString::to_string).collect();
        return Err(TlsError::SystemRoots(if errors.is_empty() {
            "none found".to_owned()
        } else {
            errors.join("; ")
        }));
    }
    Ok(roots)
}

/// Trusts any certificate, for `prefer` and `require`: the connection is encrypted, but the server is not known to be
/// the one asked for. The handshake's signatures are still checked, with the server's certificate's key.
#[derive(Debug)]
struct AnyCertificate(WebPkiSupportedAlgorithms);

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// Checks the certificate's chain to a trusted authority, but not that it is for the host connected to, for
/// `verify-ca`, and `require` with `sslrootcert`.
#[derive(Debug)]
struct AnyName(Arc<WebPkiServerVerifier>);

impl ServerCertVerifier for AnyName {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp_response, now)
        {
            Err(rustls::Error::InvalidCertificate(
                CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, iter};

    use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
    use postgres::config::Host;

    use super::*;

    /// What `split` leaves, and the `sslmode` and `sslrootcert` it took.
    fn split_ssl(url: &str) -> (String, Option<String>, Option<String>) {
        let (mut mode, mut root_cert) = (None, None);
        let rest = split(url, |key, value| {
            Ok(match key {
                "sslmode" => mode.replace(value).is_none(),
                "sslrootcert" => root_cert.replace(value).is_none(),
                _ => false,
            })
        })
        .unwrap();
        (rest, mode, root_cert)
    }

    #[test]
    fn splits_urls() {
        let url = "postgresql://user:pass@host:5432/db";
        assert_eq!(split_ssl(url), (url.to_owned(), None, None));
        assert_eq!(
            split_ssl(
                "postgres://h/db?sslmode=verify-full&sslrootcert=%2Fcerts%2Fmy%20ca.pem&connect_timeout=5"
            ),
            (
                "postgres://h/db?connect_timeout=5".to_owned(),
                Some("verify-full".to_owned()),
                Some("/certs/my ca.pem".to_owned()),
            ),
        );
        assert_eq!(
            split_ssl("postgresql://h/db?application_name=a&sslmode=require"),
            (
                "postgresql://h/db?application_name=a".to_owned(),
                Some("require".to_owned()),
                None
            ),
        );
        assert_eq!(
            split_ssl("postgresql://h/db?sslrootcert=ca.pem"),
            (
                "postgresql://h/db".to_owned(),
                None,
                Some("ca.pem".to_owned())
            ),
        );
    }

    #[test]
    fn splits_key_values() {
        assert_eq!(
            split_ssl("host=h sslmode = require   sslrootcert='/certs/my ca.pem' dbname=d"),
            (
                "host=h dbname=d".to_owned(),
                Some("require".to_owned()),
                Some("/certs/my ca.pem".to_owned()),
            ),
        );
        assert_eq!(
            split_ssl(r"  password='it\'s a b' sslrootcert=a\ b.pem user=u\ v"),
            (
                r"password='it\'s a b' user=u\ v".to_owned(),
                None,
                Some("a b.pem".to_owned())
            ),
        );
        assert_eq!(
            split_ssl("sslmode=disable"),
            (String::new(), Some("disable".to_owned()), None)
        );
        // Not `key=value`, and a quote without its end: kept for `postgres` to report
        assert_eq!(
            split_ssl("host=h oops"),
            ("host=h oops".to_owned(), None, None)
        );
        assert_eq!(
            split_ssl("host=h sslmode='require dbname=d"),
            ("host=h sslmode='require dbname=d".to_owned(), None, None),
        );
    }

    #[test]
    fn parses_modes() {
        for (url, mode, pg_mode) in [
            ("host=h", SslMode::Prefer, PgSslMode::Prefer),
            (
                "host=h sslmode=disable",
                SslMode::Disable,
                PgSslMode::Disable,
            ),
            ("host=h sslmode=prefer", SslMode::Prefer, PgSslMode::Prefer),
            (
                "postgresql://h?sslmode=require",
                SslMode::Require,
                PgSslMode::Require,
            ),
            (
                "host=h sslmode=verify-ca",
                SslMode::VerifyCa,
                PgSslMode::Require,
            ),
            (
                "postgresql://h/db?sslmode=verify-full",
                SslMode::VerifyFull,
                PgSslMode::Require,
            ),
        ] {
            let (config, parsed, root_cert) = parse(url).unwrap();
            assert_eq!(
                (parsed, config.get_ssl_mode(), root_cert),
                (mode, pg_mode, None),
                "{url}"
            );
        }
        let (config, _, root_cert) = parse("host=h dbname=d sslrootcert=/ca.pem").unwrap();
        assert_eq!(config.get_dbname(), Some("d"));
        assert_eq!(root_cert, Some(PathBuf::from("/ca.pem")));

        let err = parse("host=h sslmode=allow").unwrap_err();
        assert!(
            matches!(err, SyncError::Tls(TlsError::SslMode(ref m)) if m == "allow"),
            "{err}"
        );
        // What `postgres` does not know is still its error
        let err = parse("host=h sslcert=client.pem").unwrap_err();
        assert!(matches!(err, SyncError::Db(_)), "{err}");
    }

    #[test]
    fn rejects_unusable_roots() {
        let err = client_config(SslMode::VerifyCa, None).unwrap_err();
        assert!(matches!(err, TlsError::NoRootCert), "{err}");

        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing.pem");
        let err = client_config(SslMode::VerifyFull, Some(&missing)).unwrap_err();
        assert!(
            matches!(err, TlsError::RootCert { ref path, .. } if *path == missing),
            "{err}"
        );

        let empty = dir.path().join("empty.pem");
        fs::write(&empty, "not a certificate\n").unwrap();
        let err = client_config(SslMode::Require, Some(&empty)).unwrap_err();
        assert!(err.to_string().contains("no PEM certificate"), "{err}");

        // Not read when not needed, as libpq
        client_config(SslMode::Prefer, Some(&missing)).unwrap();
        client_config(SslMode::Require, None).unwrap();
    }

    /// The modes against the TLS server of `just test-db-up`, whose certificates are in the directory
    /// `OSMFLOW_SYNC_TEST_TLS` names, and whose role `osmflow_tls` may connect only with TLS.
    #[test]
    fn connects_with_tls() {
        let (Ok(url), Ok(certs)) = (
            env::var("OSMFLOW_SYNC_TEST_DB"),
            env::var("OSMFLOW_SYNC_TEST_TLS"),
        ) else {
            return;
        };
        let config: Config = url.parse().unwrap();
        // Windows has no `Host::Unix`
        let host = match &config.get_hosts()[0] {
            Host::Tcp(host) => host,
            #[cfg(unix)]
            Host::Unix(_) => panic!("{url} is not on TCP"),
        };
        let port = config.get_ports()[0];
        let ca = Path::new(&certs).join("ca.crt");
        let other = Path::new(&certs).join("other.crt");
        let (ca, other) = (ca.display(), other.display());
        let connect_to = |host: &str, params: &str| {
            connect(&format!(
                "host={host} port={port} user=osmflow_tls dbname=postgres {params}"
            ))
        };
        let encrypted = |client: Result<Client, SyncError>| {
            let row = client
                .unwrap()
                .query_one(
                    "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()",
                    &[],
                )
                .unwrap();
            assert!(row.get::<_, bool>(0));
        };
        let fails = |client: Result<Client, SyncError>, error: &str| {
            let err = client.err().unwrap();
            let chain: Vec<_> =
                iter::successors(Some(&err as &dyn Error), |e| (*e).source()).collect();
            let text = chain
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(": ");
            assert!(text.contains(error), "{text}");
        };

        fails(connect_to(host, "sslmode=disable"), "no encryption");
        encrypted(connect_to(host, ""));
        encrypted(connect_to(host, "sslmode=prefer"));
        encrypted(connect_to(host, "sslmode=require"));
        encrypted(connect_to(
            host,
            &format!("sslmode=require sslrootcert={ca}"),
        ));
        encrypted(connect_to(
            host,
            &format!("sslmode=verify-ca sslrootcert={ca}"),
        ));
        encrypted(connect_to(
            host,
            &format!("sslmode=verify-full sslrootcert={ca}"),
        ));

        // Another authority's certificates: `require` with them checks the issuer too
        for mode in ["require", "verify-ca", "verify-full"] {
            fails(
                connect_to(host, &format!("sslmode={mode} sslrootcert={other}")),
                "UnknownIssuer",
            );
        }
        // The server's certificate is for localhost: by its IP address, the issuer is right but the name is not
        encrypted(connect_to(
            "127.0.0.1",
            &format!("sslmode=verify-ca sslrootcert={ca}"),
        ));
        fails(
            connect_to(
                "127.0.0.1",
                &format!("sslmode=verify-full sslrootcert={ca}"),
            ),
            "not valid for name",
        );

        // The same in a URL
        let ca = utf8_percent_encode(&certs, NON_ALPHANUMERIC).to_string() + "%2Fca.crt";
        let sep = if url.contains('?') { '&' } else { '?' };
        encrypted(connect(&format!(
            "{url}{sep}user=osmflow_tls&sslmode=verify-full&sslrootcert={ca}"
        )));
    }
}
