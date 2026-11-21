//! The database side: connections, the state row, and writing rows with binary `COPY` on several connections.

use std::error::Error;
use std::io::Write as _;
use std::thread::{self, JoinHandle};

use crossbeam_channel::{Receiver, Sender, bounded};
use osmflow::Timestamp;
use postgres::types::{FromSql, Type};
use postgres::{CopyInWriter, GenericClient};

use crate::SyncError;
use crate::copy::{HEADER, Rows, TRAILER};
pub(crate) use crate::tls::connect;
use crate::varint::unpack_ids;

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
    /// The layout of `osm.nodes` and `osm.ways`.
    pub tables: TableLayout,
}

/// How `osm.nodes` and `osm.ways` store their rows: chosen by the import, kept by every update.
///
/// SQL reads `osm.nodes` and `osm.ways` with `geom` as a PostGIS geometry and `nodes` as an `int8[]` in either
/// layout. When packed, the tables are `osm.nodes_packed` and `osm.ways_packed`, and `osm.nodes` and `osm.ways` are
/// views over them that decode `geom` with `osm.geom(geom)` and `nodes` with `osm.node_ids(nodes)`. `osm.geom` is
/// inlined, so an index on `osm.ways_packed (osm.geom(geom))` serves queries on the view `osm.ways`; when plain, the
/// index is on `osm.ways (geom)`.
///
/// It displays and parses as `packed` or `plain`, its name in `osm.state`.
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
pub enum TableLayout {
    /// Compact tables, read through the views `osm.nodes` and `osm.ways`. `geom` is TWKB with OSM's 7 decimal digits
    /// in a `bytea`, which PostGIS reads exactly as the locations were written: 11.5 MB against 48.3 MB of WKB in the
    /// District of Columbia. A way's `nodes` is a `bytea` of its node IDs, each as the step from the
    /// one before (the first from 0), zigzag-encoded and written as an LEB128 varint: 6.0 MB against 25.9 MB as an
    /// `int8[]` in the District of Columbia, 17.6 MB against 86.7 MB in New York City (PostgreSQL 18 in Docker on a
    /// laptop). `osm.node_ids` decodes about 1.3 µs per node, 9.1 s for all ways of New York City against 0.28 s for
    /// reading an `int8[]`, so bulk readers should read `osm.ways_packed` and decode the bytes with [`NodeIds`].
    #[default]
    Packed,
    /// PostGIS's `geometry` with SRID 4326, stored as WKB, and `nodes` as an `int8[]`, in the tables `osm.nodes` and
    /// `osm.ways`: read without decoding.
    Plain,
}

/// The SQL of a [`TableLayout`], for `schema.sql`.
pub(crate) struct LayoutSql {
    /// The column type of a point.
    pub(crate) point: &'static str,
    /// The column type of a line string.
    pub(crate) line: &'static str,
    /// The column type of a way's node IDs.
    pub(crate) ids: &'static str,
    /// What goes after the tables: `osm.geom`, `osm.node_ids` and the views `osm.nodes` and `osm.ways` over the
    /// packed tables; nothing when plain.
    pub(crate) after: String,
}

impl TableLayout {
    /// The column types, and the functions and views, with PostGIS in schema `postgis`.
    ///
    /// The body of `osm.geom` names PostGIS's schema: Postgres reads it again where it inlines it, and builds indexes
    /// with only `pg_catalog` in the search path.
    pub(crate) fn sql(self, postgis: &str) -> LayoutSql {
        let s = format!("\"{}\"", postgis.replace('"', "\"\""));
        match self {
            Self::Packed => {
                let view = |t: Table| {
                    let columns = t
                        .columns()
                        .split(", ")
                        .map(|c| match (t, c) {
                            (Table::Ways, "nodes") => "osm.node_ids(nodes) AS nodes",
                            (_, "geom") => "osm.geom(geom) AS geom",
                            _ => c,
                        })
                        .collect::<Vec<_>>()
                        .join(", ");
                    format!(
                        "CREATE VIEW {} AS SELECT {columns} FROM {};",
                        <&str>::from(t),
                        t.name(self)
                    )
                };
                LayoutSql {
                    point: "bytea",
                    line: "bytea",
                    ids: "bytea",
                    after: format!(
                        "CREATE FUNCTION osm.geom(g bytea) RETURNS {s}.geometry LANGUAGE sql IMMUTABLE PARALLEL SAFE \
                         AS $$ SELECT {s}.ST_SetSRID({s}.ST_GeomFromTWKB(g), 4326) $$;\n{}\n{}\n{}",
                        include_str!("node_ids.sql"),
                        view(Table::Nodes),
                        view(Table::Ways)
                    ),
                }
            }
            Self::Plain => LayoutSql {
                point: "geometry(Point, 4326)",
                line: "geometry(LineString, 4326)",
                ids: "int8[]",
                after: String::new(),
            },
        }
    }
}

/// A way's node IDs from its `nodes` column, in either layout: a packed `bytea` (`osm.ways_packed`, decoded here, far
/// faster than `osm.node_ids` in SQL) or an `int8[]` (`osm.ways`). With binary `COPY`, declare the column's type as it
/// is: `bytea` when the table is packed, `int8[]` otherwise.
///
/// ```
/// use osmflow_sync::NodeIds;
/// use postgres::types::{FromSql, Type};
///
/// // Steps 100, +1, -2, +300, each zigzagged and written as a varint
/// let ids = NodeIds::from_sql(&Type::BYTEA, &[0xc8, 0x01, 0x02, 0x03, 0xd8, 0x04]).unwrap();
/// assert_eq!(ids.0, [100, 101, 99, 399]);
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NodeIds(pub Vec<i64>);

impl<'a> FromSql<'a> for NodeIds {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        if *ty == Type::BYTEA {
            Ok(Self(unpack_ids(raw)))
        } else {
            Vec::from_sql(ty, raw).map(Self)
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::BYTEA || *ty == Type::INT8_ARRAY
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
                "SELECT id, version, rev, url, sequence, (extract(epoch FROM timestamp) * 1000)::int8, tables \
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
            rev: row.get::<_, i32>(2).into(),
            url: row.get(3),
            sequence: row.get(4),
            timestamp: row.get::<_, Option<i64>>(5).map(Timestamp::from_millis),
            tables: {
                let name: &str = row.get(6);
                name.parse().map_err(|_| {
                    SyncError::state(format!("unknown table layout {name:?} in osm.state"))
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
    /// The table that stores the rows in `layout`: when packed, `osm.nodes_packed` and `osm.ways_packed`, as
    /// `osm.nodes` and `osm.ways` are views.
    pub(crate) fn name(self, layout: TableLayout) -> &'static str {
        match (self, layout) {
            (Self::Nodes, TableLayout::Packed) => "osm.nodes_packed",
            (Self::Ways, TableLayout::Packed) => "osm.ways_packed",
            _ => self.into(),
        }
    }

    /// The table's columns, in the order rows are encoded, as a list for SQL: `id` first, then the rest after `, `.
    pub(crate) fn columns(self) -> &'static str {
        match self {
            Self::Nodes => "id, changeset, ts, version, rev, tags, geom",
            Self::Ways => "id, changeset, ts, version, minor, rev, tags, nodes, geom",
            Self::Relations => {
                "id, changeset, ts, version, rev, tags, member_types, member_ids, member_roles"
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
        layout: TableLayout,
    ) -> Result<Self, SyncError> {
        let (tx, rx) = bounded(connections * 4);
        let threads = (0..connections.max(1))
            .map(|i| {
                let (url, rx) = (url.to_owned(), rx.clone());
                thread::Builder::new()
                    .name(format!("osmflow-copy-{i}"))
                    .spawn(move || write(&url, &rx, layout))
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
fn write(url: &str, rx: &Receiver<(Table, Vec<u8>)>, layout: TableLayout) -> Result<(), SyncError> {
    let mut client = connect(url)?;
    client.batch_execute("SET synchronous_commit = off; SET default_toast_compression = lz4")?;
    let mut next = rx.recv().ok();
    while let Some((table, buf)) = next {
        let mut w = copy_in(&mut client, table.name(layout), table.columns())?;
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
