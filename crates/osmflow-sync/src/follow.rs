//! Keeping a database current: updates in a loop that waits for newer diffs and stops when asked.

use std::error::Error;
use std::time::Duration;

use osmflow::osc::Replication;

use crate::db::DbState;
use crate::update::batch_end;
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
    /// The statistics returned add up the batches. [`UpdateStats::stopped`] says why a stop ended the loop, except
    /// for the time limit passing while the database is up to date, waiting for a newer diff: that is how a follow
    /// with a time limit ends when all went well, as `once` does.
    pub fn follow<E: Error + 'static>(
        &mut self,
        replication: &Replication,
        opts: &FollowOpts,
        mut after_batch: impl FnMut(&Self, &UpdateStats) -> Result<(), E>,
    ) -> Result<UpdateStats, FollowError<E>> {
        let mut total = UpdateStats {
            sequence: DbState::read(&mut self.client)?.sequence,
            ..UpdateStats::default()
        };
        loop {
            // Up to date is looked at before the stop, so a time limit that passes during the last batch ends the
            // loop as up to date, as it does while waiting
            let next = total.sequence.cast_unsigned() + 1;
            let latest = replication.state().map_err(SyncError::from)?.sequence;
            if latest < next {
                if opts.once {
                    return Ok(total);
                }
                match opts.stop.wait(opts.wait) {
                    None => continue,
                    Some(StopReason::TimeLimit) => return Ok(total),
                    Some(StopReason::Requested) => {
                        total.stopped = Some(StopReason::Requested);
                        return Ok(total);
                    }
                }
            }
            let batch = self.update(
                replication,
                &UpdateOpts {
                    max_diffs: opts.max_diffs,
                    until: Some(batch_end(next, latest, opts.max_diffs)),
                    stop: opts.stop.clone(),
                },
            )?;
            total.add(&batch);
            if batch.stopped.is_some() {
                return Ok(total);
            }
            after_batch(self, &batch).map_err(FollowError::Batch)?;
        }
    }
}
