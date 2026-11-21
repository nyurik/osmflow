# osmflow-sync

[![GitHub repo](https://img.shields.io/badge/github-nyurik/osmflow-8da0cb?logo=github)](https://github.com/nyurik/osmflow)
[![crates.io version](https://img.shields.io/crates/v/osmflow-sync)](https://crates.io/crates/osmflow-sync)
[![crate usage](https://img.shields.io/crates/d/osmflow-sync)](https://crates.io/crates/osmflow-sync)
[![docs.rs status](https://img.shields.io/docsrs/osmflow-sync)](https://docs.rs/osmflow-sync)
[![crates.io license](https://img.shields.io/crates/l/osmflow-sync)](https://github.com/nyurik/osmflow/blob/main/LICENSE-APACHE)
[![CI build status](https://github.com/nyurik/osmflow/actions/workflows/ci.yml/badge.svg)](https://github.com/nyurik/osmflow/actions)
[![Codecov](https://img.shields.io/codecov/c/github/nyurik/osmflow?component=osmflow-sync)](https://app.codecov.io/gh/nyurik/osmflow/components)

Keeps OpenStreetMap data in PostgreSQL with PostGIS: imports a PBF file, a planet or an extract, then applies the
replication diffs after it, so the database follows OpenStreetMap minute by minute, hour by hour or day by day. Every
row says when it last changed, and a log lists every change, so tables made from these can be kept current too.

The `osmflow` command runs it ([`osmflow sync`](https://github.com/nyurik/osmflow/tree/main/crates/osmflow-cli#osmflow-sync)); this crate is the library
behind it, built on [`osmflow`](https://crates.io/crates/osmflow).

```rust,no_run
use osmflow::osc::Replication;
use osmflow_sync::{ImportOpts, Mirror, UpdateOpts};

fn import_and_update() -> Result<(), Box<dyn std::error::Error>> {
    let mirror = Mirror::new("postgresql://postgres@localhost/osm", "osm-files");
    // A Geofabrik extract's header gives its replication stream and sequence
    mirror.import("district-of-columbia.osm.pbf", None, &ImportOpts::default())?;
    // The diffs after the extract's sequence, from the directory a replication server's files were saved in
    let replication = Replication::local("district-of-columbia-updates");
    let applied = mirror.update(&replication, &UpdateOpts::default())?;
    println!("now at sequence {}", applied.sequence);
    Ok(())
}
```

## What is where

| What | Where | Holds |
|---|---|---|
| `osm.nodes` | the database | tagged nodes: `id, version, changeset, ts, rev, tags jsonb, geom` (a point); a view over `osm.nodes_twkb` with TWKB |
| `osm.ways` | the database | every way: the same, with `minor`, `nodes int8[]` and `geom` (a line string with a point per node); a view over `osm.ways_twkb` with TWKB |
| `osm.relations` | the database | every relation: the same, with `member_types`, `member_ids` and `member_roles` |
| `osm.changes` | the database | each row change after the import: `rev, kind, id, action` |
| `osm.batches`, `osm.state` | the database | each revision's sequences, and where the database is in its stream |
| `nodes.cache` | the directory | every node's location, a [`PagedStore`](https://docs.rs/osmflow/latest/osmflow/nodes/struct.PagedStore.html): about 5 bytes per node |
| `ways.redb` | the directory | the reverse index: the ways through each node, in a [`redb`](https://www.redb.org) file |

* **A revision** (`rev`) numbers each transaction: 1 is the import, and each update batch adds one. A row's `rev` is
  the revision that last wrote it, so `WHERE rev > $last` finds what changed since, and `osm.changes` adds the
  deletions.
* **A way's `minor`** counts the batches that changed its geometry since its version: OSM gives a way a new version
  when its tags or node list change, not when one of its nodes moves. A moved node rewrites the geometry of every way
  through it, adds 1 to their `minor`, and logs them with action `g`. A new version of the way sets `minor` back to 0.
* **The ways through a node** come from the reverse index: [`Mirror::open`], then `ways_through`, the lookup an
  update makes itself. It may list a way that no longer has the node, after a crash between the database's commit
  and the index's, and may miss ways after a failed update until the next one succeeds.
* **`rev`, `minor` and `osm.changes` depend on the batches:** a way whose nodes move in two diffs has `minor` 2 if the
  diffs were applied in two batches, and 1 if in one, and the log lists each object once per batch. Two databases
  kept from one stream have the same rows otherwise.
* **Geometry is stored as TWKB or WKB,** as [`ImportOpts::geometry`] says, for good: TWKB with OSM's 7 decimal
  digits in a `bytea` by default, which PostGIS reads back exactly as the locations were and which takes about a
  quarter of a line's WKB, or PostGIS's `geometry`, which needs no decoding. With TWKB the tables are
  `osm.nodes_twkb` and `osm.ways_twkb`, and `osm.nodes` and `osm.ways` are views that decode `geom` with
  `osm.geom(geom)`; with WKB they are the tables. SQL reads `geom` of `osm.nodes` and `osm.ways` as a geometry in both.
  Postgres inlines the function, so `CREATE INDEX ON osm.ways_twkb USING gist (osm.geom(geom))` serves queries on
  the view; with WKB the index is `osm.ways USING gist (geom)`.
* **Geometry** is NULL for a way with fewer than two nodes, or with a node the data does not have, as at the edge of
  an extract.
* **Untagged nodes** are only in the node file. Their locations are in the ways' geometries.
* **Metadata** (`version`, `changeset`, `ts`) is NULL where the data leaves it out: Geofabrik's public files have no
  changesets. User names and IDs are not kept.

## The import's sequence

The database's place in its stream is the sequence of the last diff the file has. A Geofabrik extract's header gives
it. A planet file's header gives only its timestamp, the newest change in it, so the import reads the stream's state
files to find the last diff before the changes the file may not have: a change can show in a diff up to 6 minutes
after it is made, so the diffs from then on are applied, and the first of them may repeat changes the file has, which
changes nothing. A sequence given in [`ImportOpts`] is checked against the header's, on the stream the header names,
or else against the file's time: one after it is an error, as its diffs would be skipped. An earlier one is a
warning: its diffs repeat changes the file has, so until they are applied the database has older versions of those
objects, and the nodes they move add to their ways' `minor`.

## Updates

An update reads the diffs after the database's sequence, from a replication server through a cache directory or from
a directory, after checking that the stream is the database's: its diff at the database's sequence must have the
time the database has for it. It applies up to `max_diffs` of them (60 by default) in one transaction: of each object changed, only
its last version is written. Each batch:

1. sets the changed nodes' locations in the node file, and finds the ways through the moved ones in the reverse index;
2. reads the old node lists of those ways and of the changed ways, in one query;
3. writes the new rows with `COPY` into temporary tables, merged into the tables with one statement each, and the
   deletions, the change log, the batch and the new state, in one transaction;
4. then commits the node file and the reverse index, with the new revision as their stamp.

## Following the stream, and stopping

[`OpenMirror::follow`] applies the diffs one batch at a time and keeps doing it as the stream grows: it waits for a
newer diff when the database is up to date, or returns at once with [`FollowOpts::once`]. After each batch commits it
calls a function of yours with the mirror and the batch's statistics, the place to update tables derived from the
database before the next batch.

A [`Stop`] ends it between batches, never in the middle of one, so the database and its files stay consistent. It
asks when its time limit passes ([`Stop::with_max_runtime`]), when any thread calls [`Stop::request`], and with the
`signals` feature on Ctrl+C, SIGTERM and SIGHUP (`Stop::on_signals`, while the guard it returns lives): the first
signal finishes the batch in progress and stops; a second ends the process at once with exit status 130, rolling the
batch back, and the next update repairs what it left half done, as after a crash. osmflow prints nothing itself: the
function given to `on_signals` tells the user. [`UpdateOpts::stop`] gives a plain [`update`](Mirror::update) the same
check, and [`UpdateStats::stopped`] says why either ended early.

```rust,no_run
use std::convert::Infallible;
use std::time::Duration;

use osmflow::osc::Replication;
use osmflow_sync::{FollowOpts, Mirror, Stop};

fn follow_for_twenty_minutes() -> Result<(), Box<dyn std::error::Error>> {
    let mirror = Mirror::new("postgresql://postgres@localhost/osm", "osm-files");
    let replication = Replication::local("district-of-columbia-updates");
    let opts = FollowOpts {
        stop: Stop::new().with_max_runtime(Duration::from_secs(20 * 60)),
        ..FollowOpts::default()
    };
    let stats = mirror.open()?.follow(&replication, &opts, |_mirror, batch| {
        println!("applied {} diffs, now at sequence {}", batch.diffs, batch.sequence);
        Ok::<(), Infallible>(())
    })?;
    match stats.stopped {
        None => println!("up to date at sequence {}", stats.sequence),
        Some(reason) => println!("stopped early at sequence {}: {reason:?}", stats.sequence),
    }
    Ok(())
}
```

## The files after a crash, and when they are lost

The database commits first, and the two files after it, so a file is never ahead of the database. Each file carries
the database's identity, a random number made by the import, and the revision it is up to.

* **A file behind the database,** after a crash between the two commits, catches up when the next update opens it.
  The node file applies again the node changes of the diffs since its revision, from the cache. The reverse index adds
  again the node lists of the ways written since its revision, from the database. Entries of ways that lost a node
  stay in the index, but every way the index gives is checked against its node list, so an outdated entry only costs
  a lookup.
* **A file lost or damaged,** or ahead of a database restored from a backup, is made again by `rebuild`, which only
  reads the database. The node locations come from the database, which has every node of a way with a geometry and
  every tagged node; or from a PBF file, such as a new download, and the diffs after it up to the database's sequence,
  which gives every node. The reverse index always comes from the database.

## Connections

The database is given as libpq takes it: `postgresql://user:password@host:port/db?param=value`, or
`host=… user=… dbname=…`. Connections use TLS as its `sslmode` parameter asks, with
[rustls](https://crates.io/crates/rustls), as libpq does:

| `sslmode` | TLS | The server's certificate |
|---|---|---|
| `disable` | never | |
| `prefer`, the default | if the server has it | not checked |
| `require` | always | not checked, unless `sslrootcert` is given: then as `verify-ca` |
| `verify-ca` | always | issued by an authority in `sslrootcert`, for any host name |
| `verify-full` | always | issued by an authority in `sslrootcert`, or the system's without it, for the host connected to |

`sslrootcert` is a PEM file of the certificate authorities to trust, such as the bundle a cloud provider publishes for
its managed databases: `postgresql://user@host/osm?sslmode=verify-full&sslrootcert=/etc/ssl/db-ca.pem`. `prefer` and
`require` encrypt the connection, but cannot tell the server from another that answers in its place; `verify-full`
can. `verify-ca` needs `sslrootcert`, as the system's authorities would accept a certificate for any host. libpq's
`allow`, client certificates (`sslcert`, `sslkey`) and its environment variables (`PGSSLMODE` and the others) are not
supported, nor is its `sslrootcert=system`: `verify-full` without `sslrootcert` uses the system's authorities, where
libpq would read `~/.postgresql/root.crt`, which is never read here.

## Performance

Measured on a laptop (16 cores, NVMe disk) with PostgreSQL 18 and PostGIS 3.6 in Docker on the same machine, its
`shared_buffers` 2 GB and `max_wal_size` 16 GB:

| | District of Columbia | New York City |
|---|---|---|
| Nodes, ways, relations | 1.96 M, 286 k, 5.3 k | 5.85 M, 1.10 M, 18 k |
| Import, in all | 2.3 s | 5.5 s, with 12 connections |
| Of it: the node file | 0.1 s | 0.3 s |
| Of it: copying the rows | 1.3 s | 3.8 s |
| Of it: primary keys, `ANALYZE` and the reverse index | 0.8 s | 1.2 s |
| A day's diff, about 300 to 2,000 changes | 20 to 100 ms | |

* **The import is bound by the database:** with 2, 4, 8 and 12 connections, New York City's rows took 7.7, 5.3, 4.2
  and 3.8 s to copy. Parsing `jsonb` is the largest part of the database's work: a `COPY` of New York City's ways on
  one connection took 8.4 s with the tags as `jsonb`, and 5.2 s with them as text.
* **Settings that help an import:** a large `maintenance_work_mem` and `max_parallel_maintenance_workers` for the
  primary keys, and a large `max_wal_size`. The import turns autovacuum off on its tables while it loads them.

## Limits

* The import and the rebuilds sort the reverse index's (node, way) pairs, and a rebuild from the database its node
  locations, 16 bytes each, in runs of 1 GiB sorted on all cores, written to temporary files in the directory and
  merged, with at most three runs in memory: District of Columbia's 2.3 million pairs fit in one run in memory, and the planet's 13 billion would
  take 13 billion times 16 bytes, about 210 GB, of disk while they are sorted (computed, not measured).
* Each update is one replication stream, the one the database was imported with; the planet's minute, hour and day
  streams number their diffs each their own way.
