//! Keeping a database current: updates in a loop that waits for newer diffs and stops when asked.

use std::error::Error;
use std::time::Duration;

use osmflow::osc::Replication;

use crate::db::DbState;
use crate::{OpenMirror, Stop, StopReason, SyncError, UpdateOpts, UpdateStats};

/// How [`OpenMirror::follow`] keeps up with the replication stream.
#[derive(Debug, Clone)]
pub struct FollowOpts {
    /// The most diffs applied in one transaction, as [`UpdateOpts::max_diffs`]; 60 by default.
    pub max_diffs: u64,
    /// How long to wait for a newer diff when up to date, 60 seconds by default. Unused with `once`.
    pub wait: Duration,
    /// Return when up to date instead of waiting for newer diffs.
    pub once: bool,
    /// Asks to stop; checked before each batch and while waiting.
    pub stop: Stop,
}

impl Default for FollowOpts {
    fn default() -> Self {
        Self {
            max_diffs: 60,
            wait: Duration::from_secs(60),
            once: false,
            stop: Stop::default(),
        }
    }
}

/// Why [`OpenMirror::follow`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FollowEnd {
    /// The database had every diff the stream had: with `once` when it first did, otherwise when the time limit
    /// passed while waiting for a newer one.
    UpToDate,
    /// A stop was asked for before the next batch, which may leave the database behind the stream.
    Stopped(StopReason),
}

/// What [`OpenMirror::follow`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Followed {
    /// Why it returned.
    pub end: FollowEnd,
    /// The batches it applied, added up: `sequence` is the database's last one, and `stopped` is not set.
    pub stats: UpdateStats,
}

/// An error of [`OpenMirror::follow`]: the update's, or the one the caller's function returned.
#[derive(Debug, thiserror::Error)]
pub enum FollowError<E: Error + 'static> {
    /// Reading the stream or applying a batch failed.
    #[error(transparent)]
    Sync(#[from] SyncError),
    /// The function called after a batch failed. The batch itself was committed.
    #[error(transparent)]
    Batch(E),
}

impl OpenMirror {
    /// Applies the diffs after the database's sequence one batch at a time, and keeps doing it as the stream grows,
    /// until the database is up to date with `once`, or a stop is asked for through [`FollowOpts::stop`].
    ///
    /// After each batch commits, `after_batch` gets the mirror, to read from it, and that batch's statistics: the
    /// place for work that follows the database, such as updating tables derived from it. A stop is looked at before
    /// each batch and while waiting, so the function always runs for a batch before the loop returns. An error from
    /// it ends the loop with [`FollowError::Batch`].
    ///
    /// A stop asked for while waiting for a newer diff, with the database up to date, ends in
    /// [`FollowEnd::UpToDate`] when it is the time limit, and in [`FollowEnd::Stopped`] when it was requested.
    pub fn follow<E: Error + 'static>(
        &mut self,
        replication: &Replication,
        opts: &FollowOpts,
        mut after_batch: impl FnMut(&Self, &UpdateStats) -> Result<(), E>,
    ) -> Result<Followed, FollowError<E>> {
        let mut total = UpdateStats {
            sequence: DbState::read(&mut self.client)?.sequence,
            ..UpdateStats::default()
        };
        let one_batch = UpdateOpts {
            max_diffs: opts.max_diffs,
            until: None,
            stop: opts.stop.clone(),
        };
        loop {
            if let Some(reason) = opts.stop.reason() {
                return Ok(Followed::new(FollowEnd::Stopped(reason), total));
            }
            let next = DbState::read(&mut self.client)?.sequence.cast_unsigned() + 1;
            let latest = replication.state().map_err(SyncError::from)?.sequence;
            if latest < next {
                if opts.once {
                    return Ok(Followed::new(FollowEnd::UpToDate, total));
                }
                match opts.stop.wait(opts.wait) {
                    None => continue,
                    Some(StopReason::TimeLimit) => {
                        return Ok(Followed::new(FollowEnd::UpToDate, total));
                    }
                    Some(StopReason::Requested) => {
                        return Ok(Followed::new(
                            FollowEnd::Stopped(StopReason::Requested),
                            total,
                        ));
                    }
                }
            }
            let until = latest.min(next + opts.max_diffs.max(1) - 1);
            let batch = self.update(
                replication,
                &UpdateOpts {
                    until: Some(until),
                    ..one_batch.clone()
                },
            )?;
            if let Some(reason) = batch.stopped {
                return Ok(Followed::new(FollowEnd::Stopped(reason), total));
            }
            total.add(&batch);
            after_batch(self, &batch).map_err(FollowError::Batch)?;
        }
    }
}

impl Followed {
    fn new(end: FollowEnd, stats: UpdateStats) -> Self {
        Self { end, stats }
    }
}
