//! Asking a long update to stop: by a time limit, by code, or by Ctrl+C and SIGTERM.

#[cfg(feature = "signals")]
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "signals")]
use std::sync::{Mutex, OnceLock, PoisonError};
use std::thread;
use std::time::{Duration, Instant};

/// How long [`Stop::wait`] sleeps between looking at the clock and the request.
const WAIT_STEP: Duration = Duration::from_millis(250);

/// Why work was asked to stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// [`Stop::request`] was called: by code, or by the first Ctrl+C or SIGTERM after `Stop::on_signals`.
    Requested,
    /// The time set with [`Stop::with_max_runtime`] has passed.
    TimeLimit,
}

/// A request to stop between units of work, shared by every clone: an update checks it before each batch of diffs and
/// finishes the batch it has begun, so the database and its files stay consistent.
///
/// It stops on a time limit, on [`request`](Self::request) from any thread, and, with the `signals` feature, on
/// Ctrl+C and SIGTERM. A default `Stop` never asks.
#[derive(Debug, Clone, Default)]
pub struct Stop {
    requested: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl Stop {
    /// A stop that has not been asked for and has no time limit.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Asks to stop once `runtime` has passed, counted from now. A runtime too long to represent is no limit.
    #[must_use]
    pub fn with_max_runtime(mut self, runtime: Duration) -> Self {
        self.deadline = Instant::now().checked_add(runtime);
        self
    }

    /// Asks to stop, from any thread. It cannot be taken back.
    pub fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
    }

    /// Why to stop now, if it was requested or the time limit has passed; a request is reported first.
    #[must_use]
    pub fn reason(&self) -> Option<StopReason> {
        self.reason_at(Instant::now())
    }

    fn reason_at(&self, now: Instant) -> Option<StopReason> {
        if self.requested.load(Ordering::SeqCst) {
            Some(StopReason::Requested)
        } else if self.deadline.is_some_and(|deadline| now >= deadline) {
            Some(StopReason::TimeLimit)
        } else {
            None
        }
    }

    /// Sleeps for `duration`, or less if a stop is asked for in the meantime, and returns the reason it woke early.
    #[must_use]
    pub fn wait(&self, duration: Duration) -> Option<StopReason> {
        let end = Instant::now().checked_add(duration);
        loop {
            if let Some(reason) = self.reason() {
                return Some(reason);
            }
            let left = end.map_or(WAIT_STEP, |end| {
                end.saturating_duration_since(Instant::now())
            });
            if left.is_zero() {
                return None;
            }
            thread::sleep(left.min(WAIT_STEP));
        }
    }
}

/// What a signal means, by how many came before it.
#[cfg(feature = "signals")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    /// The first Ctrl+C or SIGTERM: a stop was requested, and the work in progress finishes first.
    First,
    /// Any later one: the process exits at once, as soon as the notification returns.
    Again,
}

/// Ctrl+C and SIGTERM could not be handled, because another handler was already installed or the system refused.
#[cfg(feature = "signals")]
#[derive(Debug, thiserror::Error)]
#[error("cannot handle Ctrl+C and SIGTERM: {0}")]
pub struct SignalError(String);

/// The exit status when a second signal ends the process: 128 plus SIGINT, as shells report Ctrl+C.
#[cfg(feature = "signals")]
const AGAIN_EXIT_STATUS: i32 = 130;

/// Who the process's one signal handler tells: the latest `Stop::on_signals`.
#[cfg(feature = "signals")]
struct Listener {
    stop: Stop,
    notify: Box<dyn Fn(Signal) + Send>,
}

#[cfg(feature = "signals")]
static LISTENER: Mutex<Option<Listener>> = Mutex::new(None);

/// The result of installing the process's signal handler, which is done once.
#[cfg(feature = "signals")]
static INSTALLED: OnceLock<Result<(), String>> = OnceLock::new();

/// The signal handler: records the signal on the listener's stop, tells it, and ends the process on a second one.
#[cfg(feature = "signals")]
fn dispatch() {
    let listener = LISTENER.lock().unwrap_or_else(PoisonError::into_inner);
    let Some(listener) = listener.as_ref() else {
        return;
    };
    let signal = listener.stop.advance();
    (listener.notify)(signal);
    if signal == Signal::Again {
        exit_now();
    }
}

#[cfg(feature = "signals")]
impl Stop {
    /// Makes Ctrl+C and SIGTERM ask this stop to stop, in two steps.
    ///
    /// The first signal calls [`request`](Self::request) and then `notify(Signal::First)`: the update finishes the
    /// batch it has begun and returns. A second signal, or one that arrives after a request made in any other way,
    /// calls `notify(Signal::Again)` and ends the process at once with exit status 130: the batch in progress is
    /// rolled back with its transaction, and the next update repairs what it left half done, as after a crash.
    ///
    /// `notify` runs on the signal-handling thread, and is the place to tell the user, as osmflow itself prints
    /// nothing. A process has one signal handler, which tells the stop of the latest call: calling this again, for
    /// the next run, replaces the earlier stop and `notify`. It fails if other code already installed a handler for
    /// these signals.
    pub fn on_signals(&self, notify: impl Fn(Signal) + Send + 'static) -> Result<(), SignalError> {
        INSTALLED
            .get_or_init(|| ctrlc::set_handler(dispatch).map_err(|e| e.to_string()))
            .clone()
            .map_err(SignalError)?;
        *LISTENER.lock().unwrap_or_else(PoisonError::into_inner) = Some(Listener {
            stop: self.clone(),
            notify: Box::new(notify),
        });
        Ok(())
    }

    /// Records a signal: the first asks to stop, and any later one, or one after a request, asks to stop at once.
    fn advance(&self) -> Signal {
        if self.requested.swap(true, Ordering::SeqCst) {
            Signal::Again
        } else {
            Signal::First
        }
    }
}

#[cfg(feature = "signals")]
#[expect(
    clippy::exit,
    reason = "a second signal ends the process at once, which is what it asks for"
)]
fn exit_now() -> ! {
    process::exit(AGAIN_EXIT_STATUS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nothing_asks_without_a_request_or_a_limit() {
        assert_eq!(Stop::new().reason(), None);
    }

    #[test]
    fn a_request_is_seen_by_every_clone() {
        let stop = Stop::new();
        let clone = stop.clone();
        assert_eq!(clone.reason(), None);
        stop.request();
        assert_eq!(clone.reason(), Some(StopReason::Requested));
    }

    #[test]
    fn the_time_limit_asks_once_it_has_passed() {
        let stop = Stop::new().with_max_runtime(Duration::from_mins(1));
        let now = Instant::now();
        assert_eq!(stop.reason_at(now), None);
        assert_eq!(stop.reason_at(now + Duration::from_secs(59)), None);
        assert_eq!(
            stop.reason_at(now + Duration::from_mins(2)),
            Some(StopReason::TimeLimit)
        );
        assert_eq!(
            Stop::new().with_max_runtime(Duration::ZERO).reason(),
            Some(StopReason::TimeLimit)
        );
    }

    #[test]
    fn a_runtime_too_long_to_count_is_no_limit() {
        assert_eq!(Stop::new().with_max_runtime(Duration::MAX).reason(), None);
    }

    #[test]
    fn a_request_is_reported_before_the_time_limit() {
        let stop = Stop::new().with_max_runtime(Duration::ZERO);
        stop.request();
        assert_eq!(stop.reason(), Some(StopReason::Requested));
    }

    #[test]
    fn waiting_ends_early_on_a_request_and_late_without_one() {
        let started = Instant::now();
        let asked = Stop::new();
        asked.request();
        assert_eq!(
            asked.wait(Duration::from_mins(1)),
            Some(StopReason::Requested)
        );
        assert!(started.elapsed() < Duration::from_secs(10));
        assert_eq!(Stop::new().wait(Duration::from_millis(30)), None);
    }

    #[test]
    fn waiting_ends_at_the_time_limit() {
        let stop = Stop::new().with_max_runtime(Duration::from_millis(30));
        assert_eq!(
            stop.wait(Duration::from_mins(1)),
            Some(StopReason::TimeLimit)
        );
    }

    #[cfg(feature = "signals")]
    #[test]
    fn the_first_signal_requests_and_every_later_one_asks_to_stop_at_once() {
        let stop = Stop::new();
        assert_eq!(stop.advance(), Signal::First);
        assert_eq!(stop.reason(), Some(StopReason::Requested));
        assert_eq!(stop.advance(), Signal::Again);
        assert_eq!(stop.advance(), Signal::Again);
    }

    #[cfg(feature = "signals")]
    #[test]
    fn the_handler_can_be_given_to_another_stop_for_the_next_run() {
        Stop::new().on_signals(|_| {}).unwrap();
        Stop::new().on_signals(|_| {}).unwrap();
    }

    #[cfg(feature = "signals")]
    #[test]
    fn a_signal_after_a_request_asks_to_stop_at_once() {
        let stop = Stop::new();
        stop.request();
        assert_eq!(stop.advance(), Signal::Again);
    }
}
