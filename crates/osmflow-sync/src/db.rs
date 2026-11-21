//! The database side: connections, the state row, and writing rows with binary `COPY` on several connections.

use std::io::Write as _;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, bounded};
use osmflow::Timestamp;
use postgres::{CopyInWriter, GenericClient};

use crate::SyncError;
use crate::copy::{HEADER, Rows, TRAILER};
pub(crate) use crate::tls::connect;

/// The format of the `osm` schema this crate reads and writes.
pub(crate) const SCHEMA_VERSION: i32 = 3;

/// Where the database is in its replication stream, from `osm.state`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbState {
    /// A random number made by the import, which the node file and the reverse index carry too.
    pub id: i64,
    /// The last revision: 1 after the import, one more after each update batch.
    pub rev: i64,
    /// The replication stream's base URL, if known.
    pub url: Option<String>,
    /// The last sequence of the stream applied.
    pub sequence: i64,
    /// The time of that sequence's state.
    pub timestamp: Option<Timestamp>,
    /// How `osm.nodes` and `osm.ways` store their geometries.
    pub geometry: GeometryFormat,
}

/// How `osm.nodes` and `osm.ways` store their geometries: chosen by the import, kept by every update.
///
/// SQL reads `geom` of `osm.nodes` and `osm.ways` as a PostGIS geometry in either format. With TWKB the tables are
/// `osm.nodes_twkb` and `osm.ways_twkb`, and `osm.nodes` and `osm.ways` are views over them that decode `geom` with
/// `osm.geom(geom)`. That function is inlined, so an index on `osm.ways_twkb (osm.geom(geom))` serves queries on the
/// view `osm.ways`; with WKB the index is on `osm.ways (geom)`.
///
/// It displays and parses as `twkb` or `wkb`, its name in `osm.state`.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    strum::AsRefStr,
    strum::Display,
    strum::EnumString,
    strum::VariantNames,
)]
#[strum(serialize_all = "lowercase")]
pub enum GeometryFormat {
    /// TWKB with OSM's 7 decimal digits in a `bytea`, which PostGIS reads exactly as the locations were written:
    /// about a quarter of the size of a line's WKB. Read through the views `osm.nodes` and `osm.ways`.
    #[default]
    Twkb,
    /// PostGIS's `geometry` with SRID 4326, stored as WKB in `osm.nodes` and `osm.ways`: read without decoding.
    Wkb,
}

/// The SQL of a [`GeometryFormat`], for `schema.sql`.
pub(crate) struct GeometrySql {
    /// The column type of a point.
    pub(crate) point: &'static str,
    /// The column type of a line string.
    pub(crate) line: &'static str,
    /// What goes after the tables: `osm.geom` and the views `osm.nodes` and `osm.ways` over the TWKB tables; nothing
    /// for WKB.
    pub(crate) after: String,
}

impl GeometryFormat {
    /// The column types, and the function and views, with PostGIS in schema `postgis`.
    ///
    /// The function's body names PostGIS's schema: Postgres reads it again where it inlines it, and builds indexes
    /// with only `pg_catalog` in the search path.
    pub(crate) fn sql(self, postgis: &str) -> GeometrySql {
        let s = format!("\"{}\"", postgis.replace('"', "\"\""));
        match self {
            Self::Twkb => {
                // `geom` is the last column
                let view = |t: Table| {
                    let rest = t.columns().strip_suffix("geom").unwrap_or_default();
                    format!(
                        "CREATE VIEW {} AS SELECT {rest}osm.geom(geom) AS geom FROM {};",
                        <&str>::from(t),
                        t.name(self)
                    )
                };
                GeometrySql {
                    point: "bytea",
                    line: "bytea",
                    after: format!(
                        "CREATE FUNCTION osm.geom(g bytea) RETURNS {s}.geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE \
                         AS $$ SELECT {s}.ST_SetSRID({s}.ST_GeomFromTWKB(g), 4326) $$;\n{}\n{}",
                        view(Table::Nodes),
                        view(Table::Ways)
                    ),
                }
            }
            Self::Wkb => GeometrySql {
                point: "geometry(Point, 4326)",
                line: "geometry(LineString, 4326)",
                after: String::new(),
            },
        }
    }
}

impl DbState {
    /// Reads the state row; an error if there is no imported database.
    pub(crate) fn read(client: &mut impl GenericClient) -> Result<Self, SyncError> {
        let exists: bool = client
            .query_one("SELECT to_regclass('osm.state') IS NOT NULL", &[])?
            .get(0);
        if !exists {
            return Err(SyncError::state(
                "the database has no osm schema: import a PBF file first",
            ));
        }
        let row = client
            .query_opt(
                "SELECT id, version, rev, url, sequence, (extract(epoch FROM timestamp) * 1000)::int8, geometry \
                 FROM osm.state",
                &[],
            )?
            .ok_or_else(|| SyncError::state("the import did not finish: run it again, replacing the osm schema"))?;
        let version: i32 = row.get(1);
        if version != SCHEMA_VERSION {
            return Err(SyncError::state(format!(
                "the osm schema has format {version}, but this osmflow-sync reads {SCHEMA_VERSION}: import again, \
                 replacing the osm schema"
            )));
        }
        Ok(Self {
            id: row.get(0),
            rev: row.get(2),
            url: row.get(3),
            sequence: row.get(4),
            timestamp: row.get::<_, Option<i64>>(5).map(Timestamp::from_millis),
            geometry: {
                let name: &str = row.get(6);
                name.parse().map_err(|_| {
                    SyncError::state(format!("unknown geometry format {name:?} in osm.state"))
                })?
            },
        })
    }
}

/// A table rows are copied into. `<&str>::from` gives the name SQL users read, `osm.nodes`; [`Table::name`] the table
/// that stores the rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "lowercase", prefix = "osm.")]
pub(crate) enum Table {
    Nodes,
    Ways,
    Relations,
}

impl Table {
    /// The table that stores the rows in `format`: with TWKB, `osm.nodes_twkb` and `osm.ways_twkb`, as `osm.nodes` and
    /// `osm.ways` are views.
    pub(crate) fn name(self, format: GeometryFormat) -> &'static str {
        match (self, format) {
            (Self::Nodes, GeometryFormat::Twkb) => "osm.nodes_twkb",
            (Self::Ways, GeometryFormat::Twkb) => "osm.ways_twkb",
            _ => self.into(),
        }
    }

    /// The table's columns, in the order rows are encoded, as a list for SQL: `id` first, then the rest after `, `.
    pub(crate) fn columns(self) -> &'static str {
        match self {
            Self::Nodes => "id, version, changeset, ts, rev, tags, geom",
            Self::Ways => "id, version, changeset, ts, minor, rev, tags, nodes, geom",
            Self::Relations => {
                "id, version, changeset, ts, rev, tags, member_types, member_ids, member_roles"
            }
        }
    }
}

/// Copies `rows` into `table` (a name, with its columns in the rows' order) in one `COPY`.
pub(crate) fn copy(
    client: &mut impl GenericClient,
    table: &str,
    columns: &str,
    rows: &Rows,
) -> Result<(), SyncError> {
    let mut w = copy_in(client, table, columns)?;
    w.write_all(&rows.buf)?;
    copy_end(w)
}

/// Starts a binary `COPY` into `table`'s `columns`: rows are written to it, then [`copy_end`] ends it.
fn copy_in<'a>(
    client: &'a mut impl GenericClient,
    table: &str,
    columns: &str,
) -> Result<CopyInWriter<'a>, SyncError> {
    let mut w = client.copy_in(&format!(
        "COPY {table} ({columns}) FROM STDIN (FORMAT binary)"
    ))?;
    w.write_all(HEADER)?;
    Ok(w)
}

fn copy_end(mut w: CopyInWriter<'_>) -> Result<(), SyncError> {
    w.write_all(TRAILER)?;
    w.finish()?;
    Ok(())
}

/// Copies rows into the tables on several connections at once, each with a `COPY` per table it is sent rows of.
pub(crate) struct Writers {
    tx: Sender<(Table, Vec<u8>)>,
    threads: Vec<JoinHandle<Result<(), SyncError>>>,
}

impl Writers {
    pub(crate) fn start(
        url: &str,
        connections: usize,
        format: GeometryFormat,
    ) -> Result<Self, SyncError> {
        let (tx, rx) = bounded(connections * 4);
        let threads = (0..connections.max(1))
            .map(|i| {
                let (url, rx) = (url.to_owned(), rx.clone());
                thread::Builder::new()
                    .name(format!("osmflow-copy-{i}"))
                    .spawn(move || write(&url, &rx, format))
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { tx, threads })
    }

    /// Hands rows to a connection; false if the connections stopped, as after an error, which
    /// [`finish`](Self::finish) returns.
    pub(crate) fn send(&self, table: Table, rows: Rows) -> bool {
        rows.buf.is_empty() || self.tx.send((table, rows.buf)).is_ok()
    }

    /// Ends every `COPY`, and returns the first error.
    pub(crate) fn finish(self) -> Result<(), SyncError> {
        drop(self.tx);
        let mut result = Ok(());
        for t in self.threads {
            let r = t
                .join()
                .unwrap_or_else(|_| Err(SyncError::state("a COPY thread panicked")));
            result = result.and(r);
        }
        result
    }
}

/// One connection's work: a `COPY` for each run of rows of one table.
fn write(
    url: &str,
    rx: &Receiver<(Table, Vec<u8>)>,
    format: GeometryFormat,
) -> Result<(), SyncError> {
    let mut client = connect(url)?;
    client.batch_execute("SET synchronous_commit = off; SET default_toast_compression = lz4")?;
    let mut next = rx.recv().ok();
    while let Some((table, buf)) = next {
        let mut w = copy_in(&mut client, table.name(format), table.columns())?;
        w.write_all(&buf)?;
        loop {
            next = rx.recv().ok();
            match &next {
                Some((t, buf)) if *t == table => w.write_all(buf)?,
                _ => break,
            }
        }
        copy_end(w)?;
    }
    Ok(())
}
