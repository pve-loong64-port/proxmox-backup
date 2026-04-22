//! JoinSet with an upper bound of concurrent tasks.
//!
//! Allows to run up to the configured number of tasks concurrently in an async
//! context.

use std::future::Future;

use tokio::task::{JoinError, JoinSet};

use proxmox_log::LogContext;

/// Run up to preconfigured number of futures concurrently on tokio tasks.
pub struct BoundedJoinSet<T> {
    /// Upper bound for concurrent task execution
    max_tasks: usize,
    /// Handles to currently spawned tasks
    workers: JoinSet<T>,
}

impl<T: Send + 'static> BoundedJoinSet<T> {
    /// Create a new join set with up to `max_task` concurrently executed tasks.
    pub fn new(max_tasks: usize) -> Self {
        Self {
            max_tasks,
            workers: JoinSet::new(),
        }
    }

    /// Spawn the given task on the workers, waiting until there is capacity to do so.
    ///
    /// If there is no capacity, this will await until there is so, returning the results
    /// for the finished task(s) providing the now free running slot in order of completion
    /// or a `JoinError` if joining failed.
    pub async fn spawn_task<F>(&mut self, task: F) -> Result<Vec<T>, JoinError>
    where
        F: Future<Output = T>,
        F: Send + 'static,
    {
        let mut results = Vec::with_capacity(self.workers.len());

        // Collect already finished task results if there are some
        while let Some(result) = self.workers.try_join_next() {
            results.push(result?);
        }

        while self.workers.len() >= self.max_tasks {
            // capacity reached, wait for an active task to complete
            if let Some(result) = self.workers.join_next().await {
                results.push(result?);
            }
        }

        match LogContext::current() {
            Some(context) => self.workers.spawn(context.scope(task)),
            None => self.workers.spawn(task),
        };

        Ok(results)
    }

    /// Waits until one of the tasks in the set completes and returns its output.
    ///
    /// Returns None if the set is empty.
    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        self.workers.join_next().await
    }

    /// Wait on all spawned tasks to run to completion.
    ///
    /// Returns the results for each task in order of completion or a `JoinError`
    /// if joining failed.
    pub async fn join_spawned_tasks(&mut self) -> Result<Vec<T>, JoinError> {
        let mut results = Vec::with_capacity(self.workers.len());

        while let Some(result) = self.workers.join_next().await {
            results.push(result?);
        }

        Ok(results)
    }
}
