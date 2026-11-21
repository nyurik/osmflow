//! Imports, updates and rebuilds against the database `OSMFLOW_SYNC_TEST_DB` names, such as
//! `postgresql://postgres@localhost:5435/postgres` (`just test-sync-db` starts one). Its `osm` schema is replaced.
//! Without it, the test does nothing.

use std::fmt::Write as _;
use std::path::Path;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;
use std::{env, fs, io, thread};

use osmflow::Location;
use osmflow::nodes::PagedStore;
use osmflow::nodes::prelude::*;
use osmflow::osc::Replication;
use osmflow_sync::{
    FileStamp, FollowError, FollowOpts, ImportOpts, Mirror, NodeIds, NodeWay, Rebuild, Stop,
    StopReason, TableLayout, UpdateOpts,
};
use postgres::{Client, NoTls};

const PBF: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../osmflow/fixtures/osmpbfreader/clipperton.osm.pbf"
);
/// The fixture's replication sequence and time.
const STATE: &str = "sequenceNumber=1708\ntimestamp=2025-04-23T20\\:21\\:01Z\n";
/// The tests share the database's `osm` schema, so they run one at a time.
static DB: Mutex<()> = Mutex::new(());

/// A node of ways 178300745 and 210476684.
const NODE: i64 = 1_886_651_627;

/// Diff 1709: [`NODE`] moves; way 265426044 and relation 11980 are deleted; a way and a tagged node are created.
const DIFF_1709: &str = r#"<modify>
  <node id="1886651627" version="9" timestamp="2026-11-01T00:00:00Z" lat="10.3" lon="-109.2"/>
</modify>
<create>
  <node id="13000000000" version="1" timestamp="2026-11-01T00:00:00Z" lat="10.31" lon="-109.21">
    <tag k="amenity" v="bench"/>
  </node>
  <way id="2000000000" version="1" timestamp="2026-11-01T00:00:00Z">
    <nd ref="1724610187"/><nd ref="13000000000"/>
    <tag k="highway" v="path"/>
  </way>
</create>
<delete>
  <way id="265426044" version="5"/>
  <relation id="11980" version="30"/>
</delete>"#;
/// Diff 1711: [`NODE`] moves again, after diff 1710 took it out of way 210476684.
const DIFF_1711: &str = r#"<modify>
  <node id="1886651627" version="10" timestamp="2026-11-01T00:02:00Z" lat="10.32" lon="-109.22"/>
</modify>"#;
/// Diff 1712: [`NODE`] moves again, and so does the bench, of way 2000000000 only.
const DIFF_1712: &str = r#"<modify>
  <node id="1886651627" version="11" timestamp="2026-11-01T00:03:00Z" lat="10.33" lon="-109.23"/>
  <node id="13000000000" version="2" timestamp="2026-11-01T00:03:00Z" lat="10.4" lon="-109.3">
    <tag k="amenity" v="bench"/>
  </node>
</modify>"#;

#[test]
fn import_update_rebuild() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    // The same imports, updates and rebuilds with each layout: one after the other, in one schema
    for tables in [TableLayout::Packed, TableLayout::Plain] {
        import_update_rebuild_as(&url, tables);
    }
}

fn import_update_rebuild_as(url: &str, tables: TableLayout) {
    let dir = tempfile::tempdir().unwrap();
    let diffs = dir.path().join("diffs");
    write_state(&diffs, 708, STATE);
    let replication = Replication::local(&diffs);
    let files = dir.path().join("files");
    let mirror = Mirror::new(url, &files);
    import(&mirror, tables);
    let mut db = Client::connect(url, NoTls).unwrap();
    assert_eq!(mirror.state().unwrap().tables, tables);
    check_geometry(&mut db, tables);
    check_metadata(&mut db);
    write_diffs(&diffs, &mut db);

    // A stream whose diff 1708 is at another time is not the database's
    let other = dir.path().join("other");
    write_state(&other, 708, &STATE.replace("20\\:21", "20\\:22"));
    let err = mirror
        .update(&Replication::local(&other), &until(1709, 1))
        .unwrap_err();
    assert!(err.to_string().contains("not the database's"), "{err}");

    // One batch per diff, with the files of revision 1 kept
    let saved = dir.path().join("saved");
    copy_dir(&files, &saved);
    let stats = mirror.update(&replication, &until(1711, 1)).unwrap();
    assert_eq!((stats.batches, stats.diffs, stats.sequence), (3, 3, 1711));
    check_rows(&mut db);
    // Moved in diffs 1709 and 1711, so changed twice; written anew by diff 1710, which took the node out
    assert_eq!(minor_rev(&mut db, 178_300_745), (2, 4));
    assert_eq!(minor_rev(&mut db, 210_476_684), (0, 3));
    let one_by_one = rows(&mut db);

    // The files of revision 1 back: the next update brings them up to revision 4, then applies diff 1712. The index
    // still lists way 210476684 for the node, which is checked and dropped, and gained way 2000000000, of the bench.
    copy_dir(&saved, &files);
    let restored = mirror.status().unwrap();
    assert_eq!(
        (restored.nodes.unwrap().rev, restored.index.unwrap().rev),
        (1, 1)
    );
    // One open mirror for the lookups and the update. Opening it brings the index up to revision 4 from the
    // database's rows, which only adds: it still lists way 210476684 for the node.
    let mut open = mirror.open().unwrap();
    assert_eq!(
        open.ways_through(&[NODE]).unwrap(),
        [way(NODE, 178_300_745), way(NODE, 210_476_684)]
    );
    let stats = open.update(&replication, &until(1712, 1)).unwrap();
    assert_eq!(stats.batches, 1);
    // The ways through two nodes, asked in any order, with a repeat and a negative ID
    let through = open.ways_through(&[13_000_000_000, NODE, NODE, -1]);
    assert_eq!(
        through.unwrap(),
        [way(NODE, 178_300_745), way(13_000_000_000, 2_000_000_000)]
    );
    drop(open);
    assert_eq!(
        changes(&mut db, 5),
        [
            ("n", 13_000_000_000, "u"),
            ("w", 178_300_745, "g"),
            ("w", 2_000_000_000, "g")
        ]
        .map(owned)
    );
    check_files(&mirror, 5);

    update_failures(tables, &mirror, &replication, &mut db, &saved, &one_by_one);

    // The same diffs in one batch: the same rows, each way changed once
    import(&mirror, tables);
    let stats = mirror.update(&replication, &until(1711, 60)).unwrap();
    assert_eq!((stats.batches, stats.diffs), (1, 3));
    assert_eq!(rows(&mut db), one_by_one);
    assert_eq!(minor_rev(&mut db, 178_300_745), (1, 2));
    mirror.update(&replication, &until(1712, 60)).unwrap();
    check_files(&mirror, 3);
    let stats = mirror.update(&replication, &until(1712, 60)).unwrap();
    assert_eq!(stats.batches, 0);

    // The files made again both ways, with the same location for every node the database has
    let located = db_node_locations(&mirror, &mut db);
    for from in [
        Rebuild::Database,
        Rebuild::Pbf {
            path: Path::new(PBF),
            sequence: None,
            replication: &replication,
            max_diffs: 2,
        },
    ] {
        fs::remove_file(mirror.index_path()).unwrap();
        mirror.rebuild(&from).unwrap();
        check_files(&mirror, 3);
        assert_same_locations(&mirror, &mut db, &located);
    }
}

/// Diff 1709 alone: a way through two nodes at one location, then a third, which PostGIS's `ST_AsTWKB` would give
/// as 2 points.
const DIFF_REPEATED: &str = r#"<create>
  <node id="13000000001" version="1" timestamp="2026-11-01T00:00:00Z" lat="10.3" lon="-109.2"/>
  <node id="13000000002" version="1" timestamp="2026-11-01T00:00:00Z" lat="10.3" lon="-109.2"/>
  <node id="13000000003" version="1" timestamp="2026-11-01T00:00:00Z" lat="10.31" lon="-109.21"/>
  <way id="2000000001" version="1" timestamp="2026-11-01T00:00:00Z">
    <nd ref="13000000001"/><nd ref="13000000002"/><nd ref="13000000003"/>
    <tag k="highway" v="path"/>
  </way>
</create>"#;

/// A database imported from the fixture file at sequence 1708, and a stream with the four diffs after it.
struct Fixture {
    _dir: tempfile::TempDir,
    mirror: Mirror,
    replication: Replication,
}

fn fixture(url: &str) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let diffs = dir.path().join("diffs");
    write_state(&diffs, 708, STATE);
    let mirror = Mirror::new(url, dir.path().join("files"));
    import(&mirror, TableLayout::Packed);
    let mut db = Client::connect(url, NoTls).unwrap();
    write_diffs(&diffs, &mut db);
    // The stream's latest state, which follow reads to know what is new: diff 1712's
    fs::write(
        diffs.join("state.txt"),
        "sequenceNumber=1712\ntimestamp=2026-11-01T00\\:03\\:30Z\n",
    )
    .unwrap();
    Fixture {
        replication: Replication::local(&diffs),
        _dir: dir,
        mirror,
    }
}

/// The database's sequence.
fn sequence(mirror: &Mirror) -> i64 {
    mirror.state().unwrap().sequence
}

#[test]
fn follow_applies_the_stream_in_batches_and_stops_when_up_to_date() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    let f = fixture(&url);
    let mut open = f.mirror.open().unwrap();
    let opts = FollowOpts {
        max_diffs: 2,
        once: true,
        ..FollowOpts::default()
    };
    let mut seen = Vec::new();
    let stats = open
        .follow(&f.replication, &opts, |_, batch| {
            seen.push((batch.diffs, batch.sequence));
            Ok::<_, io::Error>(())
        })
        .unwrap();
    assert_eq!(seen, [(2, 1710), (2, 1712)]);
    assert_eq!(
        (stats.batches, stats.diffs, stats.sequence, stats.stopped),
        (2, 4, 1712, None)
    );
    assert_eq!(sequence(&f.mirror), 1712);
    // Nothing is left, so another pass applies nothing
    let again = open
        .follow(&f.replication, &opts, |_, _| Ok::<_, io::Error>(()))
        .unwrap();
    assert_eq!((again.batches, again.stopped), (0, None));
}

#[test]
fn follow_finishes_the_batch_and_then_stops_when_requested() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    let f = fixture(&url);
    let stop = Stop::new();
    let opts = FollowOpts {
        max_diffs: 1,
        stop: stop.clone(),
        ..FollowOpts::default()
    };
    let stats = f
        .mirror
        .open()
        .unwrap()
        .follow(&f.replication, &opts, |_, _| {
            stop.request();
            Ok::<_, io::Error>(())
        })
        .unwrap();
    assert_eq!(
        (stats.batches, stats.sequence, stats.stopped),
        (1, 1709, Some(StopReason::Requested))
    );
    assert_eq!(sequence(&f.mirror), 1709);
}

#[test]
fn a_time_limit_that_has_passed_applies_nothing() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    let f = fixture(&url);
    let stop = Stop::new().with_max_runtime(Duration::ZERO);
    let opts = FollowOpts {
        stop: stop.clone(),
        ..FollowOpts::default()
    };
    let stats = f
        .mirror
        .open()
        .unwrap()
        .follow(&f.replication, &opts, |_, _| Ok::<_, io::Error>(()))
        .unwrap();
    assert_eq!(
        (stats.batches, stats.sequence, stats.stopped),
        (0, 1708, Some(StopReason::TimeLimit))
    );

    // An update stops the same way, and says why it did not reach the sequence it was asked for
    let update = UpdateOpts {
        stop,
        ..UpdateOpts::default()
    };
    let stats = f.mirror.update(&f.replication, &update).unwrap();
    assert_eq!(
        (stats.batches, stats.sequence, stats.stopped),
        (0, 1708, Some(StopReason::TimeLimit))
    );
    assert_eq!(sequence(&f.mirror), 1708);
}

#[test]
fn an_update_that_reaches_its_sequence_has_no_stop_reason() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    let f = fixture(&url);
    let stats = f.mirror.update(&f.replication, &until(1710, 1)).unwrap();
    assert_eq!(
        (stats.batches, stats.sequence, stats.stopped),
        (2, 1710, None)
    );
}

#[test]
fn follow_waits_for_newer_diffs_until_the_time_limit() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    let f = fixture(&url);
    let mut open = f.mirror.open().unwrap();
    // Long enough for the one batch to start before it, even on a slow machine
    let opts = FollowOpts {
        wait: Duration::from_millis(20),
        stop: Stop::new().with_max_runtime(Duration::from_secs(1)),
        ..FollowOpts::default()
    };
    let stats = open
        .follow(&f.replication, &opts, |_, _| Ok::<_, io::Error>(()))
        .unwrap();
    // It applied all four diffs, then waited and ended at the limit, up to date
    assert_eq!(
        (stats.diffs, stats.sequence, stats.stopped),
        (4, 1712, None)
    );
}

#[test]
fn a_time_limit_that_passes_during_the_last_batch_ends_the_follow_up_to_date() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    let f = fixture(&url);
    let mut open = f.mirror.open().unwrap();
    let stop = Stop::new().with_max_runtime(Duration::from_secs(1));
    let opts = FollowOpts {
        stop: stop.clone(),
        ..FollowOpts::default()
    };
    let stats = open
        .follow(&f.replication, &opts, |_, _| {
            // The batch has every diff, and the limit passes before the loop looks again
            while stop.reason().is_none() {
                thread::sleep(Duration::from_millis(10));
            }
            Ok::<_, io::Error>(())
        })
        .unwrap();
    assert_eq!(
        (stats.batches, stats.sequence, stats.stopped),
        (1, 1712, None)
    );
}

#[test]
fn an_error_after_a_batch_ends_the_follow_with_the_batch_committed() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    let f = fixture(&url);
    let opts = FollowOpts {
        max_diffs: 1,
        ..FollowOpts::default()
    };
    let err = f
        .mirror
        .open()
        .unwrap()
        .follow(&f.replication, &opts, |_, _| {
            Err(io::Error::other("the derived tables failed"))
        })
        .unwrap_err();
    assert!(matches!(err, FollowError::Batch(_)), "{err}");
    assert_eq!(err.to_string(), "the derived tables failed");
    assert_eq!(sequence(&f.mirror), 1709);
}

/// The node file rebuilt from the database has the locations the update gave the nodes of a way with a repeated
/// point, in either layout.
#[test]
fn rebuild_keeps_repeated_points() {
    let Ok(url) = env::var("OSMFLOW_SYNC_TEST_DB") else {
        return;
    };
    let _db = DB.lock().unwrap_or_else(PoisonError::into_inner);
    // Plain first, so the packed import replaces a plain schema, where the other test replaces a packed one
    for tables in [TableLayout::Plain, TableLayout::Packed] {
        let dir = tempfile::tempdir().unwrap();
        let diffs = dir.path().join("diffs");
        write_state(&diffs, 708, STATE);
        write_diff(&diffs, 709, DIFF_REPEATED);
        let mirror = Mirror::new(&url, dir.path().join("files"));
        import(&mirror, tables);
        mirror
            .update(&Replication::local(&diffs), &until(1709, 1))
            .unwrap();
        let mut db = Client::connect(&url, NoTls).unwrap();
        let located = db_node_locations(&mirror, &mut db);
        let at = |id| located.iter().find(|(n, _)| *n == id).map(|(_, l)| *l);
        assert_eq!(at(13_000_000_001), at(13_000_000_002), "{tables:?}");
        fs::remove_file(mirror.index_path()).unwrap();
        mirror.rebuild(&Rebuild::Database).unwrap();
        assert_same_locations(&mirror, &mut db, &located);
        // A way whose geometry has fewer points than it has nodes fails the rebuild
        let (ways, two) = match tables {
            TableLayout::Packed => (
                "osm.ways_packed",
                "ST_AsTWKB('LINESTRING(0 0,1 1)'::geometry, 7)",
            ),
            TableLayout::Plain => ("osm.ways", "'SRID=4326;LINESTRING(0 0,1 1)'::geometry"),
        };
        db.batch_execute(&format!(
            "UPDATE {ways} SET geom = {two} WHERE id = 2000000001"
        ))
        .unwrap();
        fs::remove_file(mirror.index_path()).unwrap();
        let err = mirror.rebuild(&Rebuild::Database).unwrap_err();
        assert!(err.to_string().contains("way 2000000001"), "{err}");
    }
}

/// The node file has the locations of `expected` for the nodes the database has: the differences, if not.
fn assert_same_locations(mirror: &Mirror, db: &mut Client, expected: &[(i64, Option<Location>)]) {
    let actual = db_node_locations(mirror, db);
    assert_eq!(actual.len(), expected.len());
    let differ: Vec<_> = actual
        .iter()
        .zip(expected)
        .filter(|(a, e)| a != e)
        .collect();
    assert!(differ.is_empty(), "(rebuilt, expected): {differ:?}");
}

/// The node file's location of every node the database has: those of the ways with a geometry, and the tagged nodes.
fn db_node_locations(mirror: &Mirror, db: &mut Client) -> Vec<(i64, Option<Location>)> {
    let ids = db
        .query(
            "SELECT unnest(nodes) FROM osm.ways WHERE geom IS NOT NULL \
             UNION SELECT id FROM osm.nodes WHERE geom IS NOT NULL ORDER BY 1",
            &[],
        )
        .unwrap();
    let store = PagedStore::open_read_only(mirror.node_path()).unwrap();
    ids.iter()
        .map(|r| {
            let id: i64 = r.get(0);
            (id, store.get(id.cast_unsigned()))
        })
        .collect()
}

/// Updates that fail on an open mirror, from a fresh import: a batch the database refuses, a database behind the
/// files, and a node file of another database (from the files in `saved`). `one_by_one` holds the rows after diffs
/// 1709 to 1711 applied one per batch.
fn update_failures(
    tables: TableLayout,
    mirror: &Mirror,
    replication: &Replication,
    db: &mut Client,
    saved: &Path,
    one_by_one: &[String],
) {
    // The third batch of an update fails after its moved node reached the node file, the first two committed: the
    // retry on the same mirror applies the third, moving the way through the node, and gives the same rows
    import(mirror, tables);
    let mut open = mirror.open().unwrap();
    db.batch_execute(
        "CREATE FUNCTION osm.fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE 'injected'; END $$; \
         CREATE TRIGGER fail BEFORE INSERT ON osm.batches FOR EACH ROW WHEN (NEW.rev = 4) \
         EXECUTE FUNCTION osm.fail();",
    )
    .unwrap();
    let err = open.update(replication, &until(1711, 1)).unwrap_err();
    assert!(format!("{err:?}").contains("injected"), "{err:?}");
    assert_eq!(minor_rev(db, 178_300_745), (1, 2));
    db.batch_execute("DROP TRIGGER fail ON osm.batches; DROP FUNCTION osm.fail();")
        .unwrap();
    let stats = open.update(replication, &until(1711, 1)).unwrap();
    assert_eq!(stats.batches, 1);
    assert_eq!(rows(db), one_by_one);
    assert_eq!(minor_rev(db, 178_300_745), (2, 4));
    assert_eq!(minor_rev(db, 210_476_684), (0, 3));
    // The files are checked on every update: a database behind them, as after a restore, is an error
    db.batch_execute("UPDATE osm.state SET rev = rev - 1")
        .unwrap();
    let err = open.update(replication, &until(1712, 1)).unwrap_err();
    assert!(err.to_string().contains("ahead of the database's"), "{err}");
    drop(open);
    db.batch_execute("UPDATE osm.state SET rev = rev + 1")
        .unwrap();
    // A node file of another database, under an index of this one, is an error of the first update
    for entry in fs::read_dir(saved).unwrap() {
        let name = entry.unwrap().file_name();
        if name.to_string_lossy().starts_with("nodes.cache") {
            fs::copy(saved.join(&name), mirror.node_path().with_file_name(&name)).unwrap();
        }
    }
    let err = mirror
        .open()
        .unwrap()
        .update(replication, &until(1712, 1))
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("nodes.cache was not made for this database"),
        "{err}"
    );
}

/// The tables and views the layout `tables` makes of `osm.ways`, and an index serving a bbox query on it.
fn check_geometry(db: &mut Client, tables: TableLayout) {
    let info = |db: &mut Client, table: &str, column: &str| -> Option<(String, String)> {
        let row = db
            .query_opt(
                "SELECT c.relkind::text, format_type(a.atttypid, a.atttypmod) FROM pg_class c \
                 JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = $2 WHERE c.oid = to_regclass($1)",
                &[&table, &column],
            )
            .unwrap()?;
        Some((row.get(0), row.get(1)))
    };
    let of = |kind: &str, ty: &str| Some((kind.to_owned(), ty.to_owned()));
    let index = match tables {
        TableLayout::Packed => {
            // The stored tables have the bytes, and the views decode them
            for (table, column) in [
                ("osm.ways_packed", "geom"),
                ("osm.ways_packed", "nodes"),
                ("osm.nodes_packed", "geom"),
            ] {
                assert_eq!(info(db, table, column), of("r", "bytea"), "{table}");
            }
            let geom = info(db, "osm.ways", "geom").unwrap();
            assert_eq!((geom.0.as_str(), geom.1.ends_with("geometry")), ("v", true));
            assert_eq!(info(db, "osm.ways", "nodes"), of("v", "bigint[]"));
            // Packed node lists decoded by SQL: each ID is a zigzagged varint step from the one before
            for (hex, expected) in [
                ("", &[][..]),
                ("00", &[0]),
                ("02", &[1]),
                ("01", &[-1]),
                // Steps 100, +1, -2, +300, the bytes `varint`'s unit test expects of Rust
                ("c8010203d804", &[100, 101, 99, 399]),
                // A 5-byte first ID, then small steps up and down
                (
                    "8088e2ed600215",
                    &[13_000_000_000, 13_000_000_001, 12_999_999_990],
                ),
                // Steps of 2^62 and more, up and down
                (
                    "00feffffffffffffffff01fbffffffffffffffff01",
                    &[0, i64::MAX, 1],
                ),
            ] {
                let ids: Vec<i64> = db
                    .query_one(&format!("SELECT osm.node_ids('\\x{hex}')"), &[])
                    .unwrap()
                    .get(0);
                assert_eq!(ids, expected, "{hex}");
            }
            // `NodeIds` decodes the stored bytes to the view's lists, and reads the view's `int8[]` too
            let rows = db
                .query(
                    "SELECT p.nodes, w.nodes FROM osm.ways_packed p JOIN osm.ways w USING (id)",
                    &[],
                )
                .unwrap();
            assert!(!rows.is_empty());
            for row in rows {
                let (packed, plain): (NodeIds, NodeIds) = (row.get(0), row.get(1));
                assert_eq!(packed, plain);
            }
            "osm.ways_packed USING gist (osm.geom(geom))"
        }
        TableLayout::Plain => {
            assert_eq!(
                info(db, "osm.ways", "geom"),
                of("r", "geometry(LineString,4326)")
            );
            assert_eq!(info(db, "osm.ways", "nodes"), of("r", "bigint[]"));
            assert_eq!(info(db, "osm.ways_packed", "geom"), None);
            "osm.ways USING gist (geom)"
        }
    };
    // Either way, `osm.ways` gives the node list
    let nodes: Vec<i64> = db
        .query_one("SELECT nodes FROM osm.ways WHERE id = 210476684", &[])
        .unwrap()
        .get(0);
    assert_eq!(nodes, WAY_NODES);
    // `osm.geom` is inlined, so an index on it serves a query through the view, as the database builds it with only
    // pg_catalog in the search path
    db.batch_execute(&format!(
        "CREATE INDEX ways_geom ON {index}; SET enable_seqscan = off"
    ))
    .unwrap();
    let plan = db
        .query(
            "EXPLAIN SELECT id FROM osm.ways WHERE geom && ST_MakeEnvelope(-110, 10, -109, 11, 4326)",
            &[],
        )
        .unwrap();
    let plan: Vec<String> = plan.iter().map(|r| r.get(0)).collect();
    assert!(plan.iter().any(|l| l.contains("ways_geom")), "{plan:?}");
    db.batch_execute("DROP INDEX osm.ways_geom; RESET enable_seqscan")
        .unwrap();
}

/// The nodes of way 210476684, as the PBF file gives them.
const WAY_NODES: [i64; 39] = [
    2_205_706_485,
    2_205_706_484,
    2_205_706_481,
    2_712_288_731,
    2_712_288_732,
    2_712_288_734,
    2_712_288_737,
    2_712_288_739,
    2_712_288_738,
    2_712_288_736,
    2_712_288_735,
    2_712_288_733,
    2_712_288_730,
    2_712_288_729,
    2_710_937_390,
    2_205_706_478,
    2_205_706_475,
    2_205_706_460,
    2_205_706_468,
    2_205_706_472,
    2_205_706_464,
    1_886_651_646,
    2_205_706_444,
    2_205_706_452,
    2_710_937_359,
    2_205_706_456,
    1_886_651_627,
    1_886_651_588,
    1_886_651_677,
    2_710_937_365,
    2_710_937_364,
    2_710_937_367,
    2_710_937_369,
    1_724_610_187,
    2_710_937_373,
    2_710_937_377,
    2_710_937_380,
    2_710_937_391,
    2_205_706_485,
];

fn import(mirror: &Mirror, tables: TableLayout) {
    let opts = ImportOpts {
        replace: true,
        tables,
        ..ImportOpts::default()
    };
    let stats = mirror.import(PBF, None, &opts).unwrap();
    assert_eq!(
        (stats.nodes, stats.tagged_nodes, stats.ways, stats.relations),
        (3097, 238, 151, 17)
    );
}

fn way(node: i64, way: i64) -> NodeWay {
    NodeWay { node, way }
}

fn until(sequence: u64, max_diffs: u64) -> UpdateOpts {
    UpdateOpts {
        max_diffs,
        until: Some(sequence),
        ..UpdateOpts::default()
    }
}

/// Writes the state file of diff `1000 + n` into the stream directory `dir`.
fn write_state(dir: &Path, n: u32, state: &str) {
    fs::create_dir_all(dir.join("000/001")).unwrap();
    fs::write(dir.join(format!("000/001/{n}.state.txt")), state).unwrap();
}

/// Writes diffs 1709 to 1712. Diff 1710 takes [`NODE`] out of way 210476684, whose other nodes come from the database.
fn write_diffs(dir: &Path, db: &mut Client) {
    let nodes: Vec<i64> = db
        .query_one("SELECT nodes FROM osm.ways WHERE id = 210476684", &[])
        .unwrap()
        .get(0);
    let refs = nodes
        .iter()
        .filter(|&&n| n != NODE)
        .fold(String::new(), |mut refs, n| {
            write!(refs, r#"<nd ref="{n}"/>"#).unwrap();
            refs
        });
    let diff_1710 = format!(
        r#"<modify><way id="210476684" version="99" timestamp="2026-11-01T00:01:00Z">{refs}</way></modify>"#
    );
    for (n, diff) in [
        (709, DIFF_1709),
        (710, &diff_1710),
        (711, DIFF_1711),
        (712, DIFF_1712),
    ] {
        write_diff(dir, n, diff);
    }
}

/// Writes diff `1000 + n`, of the changes `diff`, and its state, a minute after diff 1709's for each diff after it.
fn write_diff(dir: &Path, n: u32, diff: &str) {
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<osmChange version=\"0.6\">\n{diff}\n</osmChange>\n"
    );
    fs::write(dir.join(format!("000/001/{n}.osc.gz")), xml).unwrap();
    let state = format!(
        "sequenceNumber={}\ntimestamp=2026-11-01T00\\:0{}\\:30Z\n",
        1000 + n,
        n - 709
    );
    write_state(dir, n, &state);
}

/// Makes directory `to` a copy of the files of `from`.
fn copy_dir(from: &Path, to: &Path) {
    if to.exists() {
        fs::remove_dir_all(to).unwrap();
    }
    fs::create_dir_all(to).unwrap();
    for entry in fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        if path.is_file() {
            fs::copy(&path, to.join(path.file_name().unwrap())).unwrap();
        }
    }
}

/// The metadata columns of a node, a way and a relation just imported, as `dump --metadata` shows them in the
/// fixture; the file has no changesets.
fn check_metadata(db: &mut Client) {
    let ts = "to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')";
    for (table, id, version, time) in [
        ("nodes", 2_205_706_315_i64, 8, "2020-11-09T19:34:27Z"),
        ("ways", 178_300_745, 4, "2025-01-16T08:49:46Z"),
        ("relations", 11980, 752, "2024-05-25T19:42:49Z"),
    ] {
        let row = db
            .query_one(
                &format!("SELECT version, changeset, {ts}, rev FROM osm.{table} WHERE id = $1"),
                &[&id],
            )
            .unwrap();
        let got: (i32, Option<i64>, String, i32) = (row.get(0), row.get(1), row.get(2), row.get(3));
        assert_eq!(got, (version, None, time.to_owned(), 1), "{table} {id}");
    }
    let minor: i32 = db
        .query_one("SELECT minor FROM osm.ways WHERE id = 178300745", &[])
        .unwrap()
        .get(0);
    assert_eq!(minor, 0);
}

fn minor_rev(db: &mut Client, id: i64) -> (i32, i32) {
    let row = db
        .query_one("SELECT minor, rev FROM osm.ways WHERE id = $1", &[&id])
        .unwrap();
    (row.get(0), row.get(1))
}

/// Every row of the three tables, without `minor` and `rev`, which depend on the batches.
fn rows(db: &mut Client) -> Vec<String> {
    [
        "SELECT format('n%s %s %s %s', id, version, tags, ST_AsText(geom)) FROM osm.nodes ORDER BY id",
        "SELECT format('w%s %s %s %s %s', id, version, tags, nodes, ST_AsText(geom)) FROM osm.ways ORDER BY id",
        "SELECT format('r%s %s %s %s %s', id, version, tags, member_ids, member_roles) FROM osm.relations ORDER BY id",
    ]
    .iter()
    .flat_map(|sql| db.query(*sql, &[]).unwrap())
    .map(|r| r.get(0))
    .collect()
}

/// The change log of revision `rev`.
fn changes(db: &mut Client, rev: i32) -> Vec<(String, i64, String)> {
    db.query(
        "SELECT kind::text, id, action::text FROM osm.changes WHERE rev = $1 ORDER BY 1, 2",
        &[&rev],
    )
    .unwrap()
    .iter()
    .map(|r| (r.get(0), r.get(1), r.get(2)))
    .collect()
}

fn owned((kind, id, action): (&str, i64, &str)) -> (String, i64, String) {
    (kind.to_owned(), id, action.to_owned())
}

/// The rows and the change log after diffs 1709 to 1711, one per batch.
fn check_rows(db: &mut Client) {
    let geom = |db: &mut Client, id: i64| -> Option<String> {
        db.query_opt("SELECT ST_AsText(geom) FROM osm.ways WHERE id = $1", &[&id])
            .unwrap()
            .map(|r| r.get(0))
    };
    assert!(geom(db, 178_300_745).unwrap().contains("-109.22 10.32"));
    assert!(!geom(db, 210_476_684).unwrap().contains("-109.2 "));
    assert_eq!(geom(db, 265_426_044), None);
    assert!(geom(db, 2_000_000_000).unwrap().ends_with("-109.21 10.31)"));
    let bench: i64 = db
        .query_one(
            "SELECT count(*) FROM osm.nodes WHERE tags->>'amenity' = 'bench'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(bench, 1);
    let expected = [
        ("n", 13_000_000_000, "u"),
        ("r", 11980, "d"),
        ("w", 178_300_745, "g"),
        ("w", 210_476_684, "g"),
        ("w", 265_426_044, "d"),
        ("w", 2_000_000_000, "u"),
    ];
    assert_eq!(changes(db, 2), expected.map(owned));
    assert_eq!(changes(db, 3), [("w", 210_476_684, "u")].map(owned));
    assert_eq!(changes(db, 4), [("w", 178_300_745, "g")].map(owned));
}

/// The files are at revision `rev`, with the nodes where the last diff put them.
fn check_files(mirror: &Mirror, rev: i64) {
    let status = mirror.status().unwrap();
    let stamp = FileStamp {
        id: Some(status.db.id),
        rev,
    };
    assert_eq!(status.nodes.unwrap(), stamp);
    assert_eq!(status.index.unwrap(), stamp);
    let store = PagedStore::open_read_only(mirror.node_path()).unwrap();
    for (id, lat, lon) in [(NODE, 10.33, -109.23), (13_000_000_000, 10.4, -109.3)] {
        let id = id.cast_unsigned();
        assert_eq!(
            store.get(id),
            Some(Location::from_degrees(lat, lon)),
            "{id}"
        );
    }
}
