#![cfg_attr(test, deny(warnings))]
#![deny(missing_docs)]

//! # scoped-pool
//!
//! A flexible thread pool providing scoped threads.
//! This has been replicated from <`https://github.com/reem/rust-scoped-pool`> to support recent versions of crossbeam
//!

use variance::InvariantLifetime as Id;
use std::{thread, mem};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Condvar};
use scopeguard::defer;

/// A thread-pool providing scoped and unscoped threads.
///
/// The primary ways of interacting with the `Pool` are
/// the `spawn` and `scoped` convenience methods or through
/// the `Scope` type directly.
#[derive(Clone, Default)]
pub struct Pool {
    wait: Arc<WaitGroup>,
    inner: Arc<PoolInner>
}

impl Pool {
    /// Create a new Pool with `size` threads.
    ///
    /// If `size` is zero, no threads will be spawned. Threads can
    /// be added later via `expand`.
    ///
    /// NOTE: Since Pool can be freely cloned, it does not represent a unique
    /// handle to the thread pool. As a consequence, the thread pool is not
    /// automatically shut down; you must explicitly call `Pool::shutdown` to
    /// shut down the pool.
    #[inline]
    pub fn new(size: usize) -> Pool {
        // Create an empty pool.
        let pool = Pool::empty();

        // Start the requested number of threads.
        for _ in 0..size { pool.expand(); }

        pool
    }

    /// Create an empty Pool, with no threads.
    ///
    /// Note that no jobs will run until `expand` is called and
    /// worker threads are added.
    #[inline]
    pub fn empty() -> Pool {
        Pool::default()
    }

    /// Spawn a `'static'` job to be run on this pool.
    ///
    /// We do not wait on the job to complete.
    ///
    /// Panics in the job will propogate to the calling thread.
    #[inline]
    pub fn spawn<F: FnOnce() + Send + 'static>(&self, job: F) {
        // Run the job on a scope which lasts forever, and won't block.
        Scope::forever(self.clone()).execute(job);
    }

    /// Create a Scope for scheduling a group of jobs in `'scope'`.
    ///
    /// `scoped` will return only when the `scheduler` function and
    /// all jobs queued on the given Scope have been run.
    ///
    /// Panics in any of the jobs or in the scheduler function itself
    /// will propogate to the calling thread.
    #[inline]
    pub fn scoped<'scope, F, R>(&self, scheduler: F) -> R
    where F: FnOnce(&Scope<'scope>) -> R {
        // Zoom to the correct scope, then run the scheduler.
        Scope::forever(self.clone()).zoom(scheduler)
    }

    /// Shutdown the Pool.
    ///
    /// WARNING: Extreme care should be taken to not call shutdown concurrently
    /// with any scoped calls, or deadlock can occur.
    ///
    /// All threads will be shut down eventually, but only threads started before the
    /// call to shutdown are guaranteed to be shut down before the call to shutdown
    /// returns.
    #[inline]
    pub fn shutdown(&self) {
        // Start the shutdown process.
        self.inner.queue_sender.send(PoolMessage::Quit).unwrap();

        // Wait for it to complete.
        self.wait.join();
    }

    /// Expand the Pool by spawning an additional thread.
    ///
    /// Can accelerate the completion of running jobs.
    #[inline]
    pub fn expand(&self) {
        let pool = self.clone();

        // Submit the new thread to the thread waitgroup.
        pool.wait.submit();

        let thread_number = self.inner.thread_counter.fetch_add(1, Ordering::SeqCst);

        // Deal with thread configuration.
        let mut builder = thread::Builder::new();
        if let Some(ref prefix) = self.inner.thread_config.prefix {
            let name = format!("{}{}", prefix, thread_number);
            builder = builder.name(name);
        }
        if let Some(stack_size) = self.inner.thread_config.stack_size {
            builder = builder.stack_size(stack_size);
        }

        // Start the actual thread.
        builder.spawn(move || pool.run_thread()).unwrap();
    }

    fn run_thread(self) {
        // Create a sentinel to capture panics on this thread.
        let mut thread_sentinel = ThreadSentinel(Some(self.clone()));

        loop {
            match self.inner.queue_receiver.recv().unwrap() {
                // On Quit, repropogate and quit.
                PoolMessage::Quit => {
                    // Repropogate the Quit message to other threads.
                    self.inner.queue_sender.send(PoolMessage::Quit).unwrap();

                    // Cancel the thread sentinel so we don't panic waiting
                    // shutdown threads, and don't restart the thread.
                    thread_sentinel.cancel();

                    // Terminate the thread.
                    break
                },

                // On Task, run the task then complete the WaitGroup.
                PoolMessage::Task(job, wait) => {
                    let sentinel = Sentinel(self.clone(), Some(wait.clone()));
                    job.run();
                    sentinel.cancel();
                }
            }
        }
    }
}

struct PoolInner {
    queue_sender: crossbeam_channel::Sender<PoolMessage>,
    queue_receiver: crossbeam_channel::Receiver<PoolMessage>,
    thread_config: ThreadConfig,
    thread_counter: AtomicUsize
}

impl Default for PoolInner {
    fn default() -> Self {
        let (queue_sender, queue_receiver) = crossbeam_channel::unbounded();
        PoolInner {
            queue_sender,
            queue_receiver,
            thread_config: ThreadConfig::default(),
            thread_counter: AtomicUsize::new(1)
        }
    }
}

/// Thread configuration. Provides detailed control over the properties and behavior of new
/// threads.
#[derive(Default)]
pub struct ThreadConfig {
    prefix: Option<String>,
    stack_size: Option<usize>,
}

/// An execution scope, represents a set of jobs running on a Pool.
///
/// ## Understanding Scope lifetimes
///
/// Besides `Scope<'static>`, all `Scope` objects are accessed behind a
/// reference of the form `&'scheduler Scope<'scope>`.
///
/// `'scheduler` is the lifetime associated with the *body* of the
/// "scheduler" function (functions passed to `zoom`/`scoped`).
///
/// `'scope` is the lifetime which data captured in `execute` or `recurse`
/// closures must outlive - in other words, `'scope` is the maximum lifetime
/// of all jobs scheduler on a `Scope`.
///
/// Note that since `'scope: 'scheduler` (`'scope` outlives `'scheduler`)
/// `&'scheduler Scope<'scope>` can't be captured in an `execute` closure;
/// this is the reason for the existence of the `recurse` API, which will
/// inject the same scope with a new `'scheduler` lifetime (this time set
/// to the body of the function passed to `recurse`).
pub struct Scope<'scope> {
    pool: Pool,
    wait: Arc<WaitGroup>,
    _scope: Id<'scope>
}

impl<'scope> Scope<'scope> {
    /// Create a Scope which lasts forever.
    #[inline]
    pub fn forever(pool: Pool) -> Scope<'static> {
        Scope {
            pool,
            wait: Arc::new(WaitGroup::new()),
            _scope: Id::default()
        }
    }

    /// Add a job to this scope.
    ///
    /// Subsequent calls to `join` will wait for this job to complete.
    pub fn execute<F>(&self, job: F)
    where F: FnOnce() + Send + 'scope {
        // Submit the job *before* submitting it to the queue.
        self.wait.submit();

        let task = unsafe {
            // Safe because we will ensure the task finishes executing before
            // 'scope via joining before the resolution of `'scope`.
            mem::transmute::<Box<dyn Task + Send + 'scope>,
                             Box<dyn Task + Send + 'static>>(Box::new(job))
        };

        // Submit the task to be executed.
        self.pool.inner.queue_sender.send(PoolMessage::Task(task, self.wait.clone())).unwrap();
    }

    /// Create a new subscope, bound to a lifetime smaller than our existing Scope.
    ///
    /// The subscope has a different job set, and is joined before zoom returns.
    pub fn zoom<'smaller, F, R>(&self, scheduler: F) -> R
    where F: FnOnce(&Scope<'smaller>) -> R,
          'scope: 'smaller {
        let scope: Scope<'smaller> = unsafe { self.refine() };

        // Join the scope either on completion of the scheduler or panic.
        defer!(scope.join());

        // Schedule all tasks then join all tasks
        scheduler(&scope)
    }

    /// Awaits all jobs submitted on this Scope to be completed.
    ///
    /// Only guaranteed to join jobs which where `execute`d logically
    /// prior to `join`. Jobs `execute`d concurrently with `join` may
    /// or may not be completed before `join` returns.
    #[inline]
    pub fn join(&self) {
        self.wait.join();
    }

    // Create a new scope with a smaller lifetime on the same pool.
    #[inline]
    unsafe fn refine<'other>(&self) -> Scope<'other> where 'scope: 'other {
        Scope {
            pool: self.pool.clone(),
            wait: Arc::new(WaitGroup::new()),
            _scope: Id::default()
        }
    }
}

enum PoolMessage {
    Quit,
    Task(Box<(dyn Task + Send)>, Arc<WaitGroup>)
}

/// A synchronization primitive for awaiting a set of actions.
///
/// Adding new jobs is done with `submit`, jobs are completed with `complete`,
/// and any thread may wait for all jobs to be `complete`d with `join`.
pub struct WaitGroup {
    pending: AtomicUsize,
    poisoned: AtomicBool,
    lock: Mutex<()>,
    cond: Condvar
}

impl Default for WaitGroup {
    fn default() -> Self {
        WaitGroup {
            pending: AtomicUsize::new(0),
            poisoned: AtomicBool::new(false),
            lock: Mutex::new(()),
            cond: Condvar::new()
        }
    }
}

impl WaitGroup {
    /// Create a new empty ``WaitGroup``
    #[inline]
    pub fn new() -> Self {
        WaitGroup::default()
    }

    /// Submit to this ``WaitGroup``, causing `join` to wait
    /// for an additional `complete`.
    #[inline]
    pub fn submit(&self) {
        self.pending.fetch_add(1, Ordering::SeqCst);
    }

    /// Complete a previous `submit`.
    #[inline]
    pub fn complete(&self) {
        // Mark the current job complete.
        let old = self.pending.fetch_sub(1, Ordering::SeqCst);

        // If that was the last job, wake joiners.
        if old == 1 {
            let _lock = self.lock.lock().unwrap();
            self.cond.notify_all();
        }
    }

    /// Poison the ``WaitGroup`` so all `join`ing threads panic.
    #[inline]
    pub fn poison(&self) {
        // Poison the waitgroup.
        self.poisoned.store(true, Ordering::SeqCst);

        // Mark the current job complete.
        let old = self.pending.fetch_sub(1, Ordering::SeqCst);

        // If that was the last job, wake joiners.
        if old == 1 {
            let _lock = self.lock.lock().unwrap();
            self.cond.notify_all();
        }
    }

    /// Wait for `submit`s to this ``WaitGroup`` to be `complete`d.
    ///
    /// Submits occuring completely before joins will always be waited on.
    ///
    /// Submits occuring concurrently with a `join` may or may not
    /// be waited for.
    ///
    /// Before submitting, `join` will always return immediately.
    #[inline]
    pub fn join(&self) {
        let mut lock = self.lock.lock().unwrap();

        while self.pending.load(Ordering::SeqCst) > 0 {
            lock = self.cond.wait(lock).unwrap();
        }

        assert!(!self.poisoned.load(Ordering::SeqCst), "WaitGroup explicitly poisoned!");
    }
}

// Poisons the given pool on drop unless canceled.
//
// Used to ensure panic propogation between jobs and waiting threads.
struct Sentinel(Pool, Option<Arc<WaitGroup>>);

impl Sentinel {
    fn cancel(mut self) {
        if let Some(wait) = self.1.take() {
            wait.complete();
        }
    }
}

impl Drop for Sentinel {
    fn drop(&mut self) {
        if let Some(wait) = self.1.take() {
            wait.poison();
        }
    }
}

struct ThreadSentinel(Option<Pool>);

impl ThreadSentinel {
    fn cancel(&mut self) {
        if let Some(pool) = self.0.take() {
            pool.wait.complete();
        }
    }
}

impl Drop for ThreadSentinel {
    fn drop(&mut self) {
        if let Some(pool) = self.0.take() {
            // NOTE: We restart the thread first so we don't accidentally
            // hit zero threads before restarting.

            // Restart the thread.
            pool.expand();

            // Poison the pool.
            pool.wait.poison();
        }
    }
}

trait Task {
    fn run(self: Box<Self>);
}

impl<F: FnOnce()> Task for F {
    fn run(self: Box<Self>) { (*self)() }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    use std::thread::sleep;

    use super::*;

    #[test]
    fn test_simple_use() {
        let pool = Pool::new(4);

        let mut buf = [0, 0, 0, 0];

        pool.scoped(|scope| {
            for i in &mut buf {
                scope.execute(move || *i += 1);
            }
        });

        assert_eq!(&buf, &[1, 1, 1, 1]);
    }

    #[test]
    fn test_zoom() {
        let pool = Pool::new(4);

        let mut outer = 0;

        pool.scoped(|scope| {
            let mut inner = 0;
            scope.zoom(|scope2| scope2.execute(|| inner = 1));
            assert_eq!(inner, 1);

            outer = 1;
        });

        assert_eq!(outer, 1);
    }


    #[test]
    fn test_spawn_doesnt_hang() {
        let pool = Pool::new(1);
        pool.spawn(move || loop {std::thread::sleep(std::time::Duration::from_secs(1));});
    }

    #[test]
    fn test_forever_zoom() {
        let pool = Pool::new(16);
        let forever = Scope::forever(pool);

        let ran = AtomicBool::new(false);

        forever.zoom(|scope| scope.execute(|| ran.store(true, Ordering::SeqCst)));

        assert!(ran.load(Ordering::SeqCst));
    }

    #[test]
    fn test_shutdown() {
        let pool = Pool::new(4);
        pool.shutdown();
    }

    #[test]
    #[should_panic]
    fn test_scheduler_panic() {
        let pool = Pool::new(4);
        pool.scoped(|_| panic!());
    }

    #[test]
    #[should_panic]
    fn test_scoped_execute_panic() {
        let pool = Pool::new(4);
        pool.scoped(|scope| scope.execute(|| panic!()));
    }

    #[test]
    #[should_panic]
    fn test_pool_panic() {
        let _pool = Pool::new(1);
        panic!();
    }

    #[test]
    #[should_panic]
    fn test_zoomed_scoped_execute_panic() {
        let pool = Pool::new(4);
        pool.scoped(|scope| scope.zoom(|scope2| scope2.execute(|| panic!())));
    }

    struct Canary<'a> {
        drops: DropCounter<'a>,
        expected: usize
    }

    #[derive(Clone)]
    struct DropCounter<'a>(&'a AtomicUsize);

    impl<'a> Drop for DropCounter<'a> {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl<'a> Drop for Canary<'a> {
        fn drop(&mut self) {
            let drops = self.drops.0.load(Ordering::SeqCst);
            assert_eq!(drops, self.expected);
        }
    }

    #[test]
    #[should_panic]
    fn test_scoped_panic_waits_for_all_tasks() {
        let tasks = 50;
        let panicking_task_fraction = 10;
        let panicking_tasks = tasks / panicking_task_fraction;
        let expected_drops = tasks + panicking_tasks;

        let counter = Box::new(AtomicUsize::new(0));
        let drops = DropCounter(&*counter);

        // Actual check occurs on drop of this during unwinding.
        let _canary = Canary {
            drops: drops.clone(),
            expected: expected_drops
        };

        let pool = Pool::new(12);

        pool.scoped(|scope| {
            for task in 0..tasks {
                let drop_counter = drops.clone();

                scope.execute(move || {
                    sleep(Duration::from_millis(10));

                    drop::<DropCounter>(drop_counter);
                });

                if task % panicking_task_fraction == 0 {
                    let drop_counter = drops.clone();

                    scope.execute(move || {
                        // Just make sure we capture it.
                        let _drops = drop_counter;
                        panic!();
                    });
                }
            }
        });
    }

    #[test]
    #[should_panic]
    fn test_scheduler_panic_waits_for_tasks() {
        let tasks = 50;
        let counter = Box::new(AtomicUsize::new(0));
        let drops = DropCounter(&*counter);

        let _canary = Canary {
            drops: drops.clone(),
            expected: tasks
        };

        let pool = Pool::new(12);

        pool.scoped(|scope| {
            for _ in 0..tasks {
                let drop_counter = drops.clone();

                scope.execute(move || {
                    sleep(Duration::from_millis(25));
                    drop::<DropCounter>(drop_counter);
                });
            }

            panic!();
        });
    }

    #[test]
    fn test_no_thread_config() {
        let pool = Pool::new(1);

        pool.scoped(|scope| {
            scope.execute(|| {
                assert!(::std::thread::current().name().is_none());
            });
        });
    }
}