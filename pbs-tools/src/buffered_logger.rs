//! Log aggregator to collect and group messages sent from concurrent tasks via
//! a tokio channel.

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Duration;

use anyhow::Error;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, error, info, trace, warn, Level};

use proxmox_log::LogContext;

/// Label to be used to group buffered messages when flushing.
pub type SenderLabel = String;

/// Requested action for the log collection task
enum SenderRequest {
    /// New log line to be buffered
    Message(SenderLabel, LogLine),
    /// Flush currently buffered log lines associated by sender label
    Flush(SenderLabel),
    /// Closes the channel, flushing all buffered log lines and preventing further logging
    Close,
}

/// Receives log lines tagged with a label, and buffers them grouped
/// by the label value. Buffered messages are flushed either after
/// reaching a certain timeout or capacity limit, or when explicitly
/// requested.
pub struct BufferedLogger {
    /// Buffer to aggregate log lines and last message received instant based on sender label
    buffer_map: HashMap<SenderLabel, (Instant, Vec<LogLine>)>,
    /// Maximum number of received lines for an individual sender instance before
    /// flushing
    max_buffered_lines: usize,
    /// Maximum aggregation duration of received lines for an individual sender
    /// instance before flushing
    max_aggregation_time: Duration,
    /// Channel to receive log messages
    receiver: mpsc::Receiver<SenderRequest>,
}

/// Instance to create new sender instances by cloning the channel sender
pub struct LogLineSenderBuilder {
    // Used to clone new senders if requested
    sender: Option<mpsc::Sender<SenderRequest>>,
}

impl LogLineSenderBuilder {
    /// Create new sender instance to send log messages, to be grouped by given label
    ///
    /// Label is not checked to be unique (no other instance with same label exists),
    /// it is the callers responsibility to check so if required.
    pub fn sender_with_label(&self, label: SenderLabel) -> LogLineSender {
        LogLineSender {
            label,
            sender: self.sender.clone(),
        }
    }

    /// Closes the channel, flushing all pending messages and closing the receiver
    pub async fn close(self) -> Result<(), Error> {
        if let Some(sender) = self.sender {
            sender.send(SenderRequest::Close).await?;
            // wait for flushing and dropping of receiver
            sender.closed().await;
        }

        Ok(())
    }
}

/// Sender to send new log messages to buffered log aggregator
pub struct LogLineSender {
    /// Label used to group log lines
    label: SenderLabel,
    /// Sender to publish new log lines to buffered log aggregator task
    sender: Option<mpsc::Sender<SenderRequest>>,
}

impl LogLineSender {
    /// Send a new log message with given level to the buffered logger task
    pub async fn log(&self, level: Level, message: String) -> Result<(), Error> {
        let line = LogLine { level, message };
        if let Some(sender) = &self.sender {
            sender
                .send(SenderRequest::Message(self.label.clone(), line))
                .await?;
        } else {
            BufferedLogger::log_by_level(&self.label, &line);
        }
        Ok(())
    }

    /// Request flushing of all buffered messages with this sender's label
    pub async fn flush(&self) -> Result<(), Error> {
        if let Some(sender) = &self.sender {
            sender
                .send(SenderRequest::Flush(self.label.clone()))
                .await?;
        }
        Ok(())
    }
}

/// Log message entity
struct LogLine {
    /// Log level of the log message
    level: Level,
    /// Log message
    message: String,
}

impl BufferedLogger {
    /// New instance of a buffered logger
    pub fn new(max_buffered_lines: usize, max_aggregation_time: Duration) -> LogLineSenderBuilder {
        if max_buffered_lines > 0 || !max_aggregation_time.is_zero() {
            let (sender, receiver) = mpsc::channel(100);

            let logger = Self {
                buffer_map: HashMap::new(),
                max_buffered_lines,
                max_aggregation_time,
                receiver,
            };
            logger.run_log_collection();
            LogLineSenderBuilder {
                sender: Some(sender),
            }
        } else {
            LogLineSenderBuilder { sender: None }
        }
    }

    /// Starts the collection loop spawned on a new tokio task
    /// Finishes when all sender belonging to the channel have been dropped.
    fn run_log_collection(mut self) {
        let future = async move {
            loop {
                for (label, (last_logged, log_lines)) in self.buffer_map.iter_mut() {
                    if !log_lines.is_empty()
                        && Instant::now().duration_since(*last_logged) > self.max_aggregation_time
                    {
                        Self::flush_by_label(label, log_lines);
                    }
                }

                match tokio::time::timeout(self.max_aggregation_time, self.receive_log_line()).await
                {
                    Ok(finished) => {
                        if finished {
                            break;
                        }
                    }
                    Err(_timeout) => self.flush_all_buffered(),
                }
            }
        };
        match LogContext::current() {
            None => tokio::spawn(future),
            Some(context) => tokio::spawn(context.scope(future)),
        };
    }

    /// Collects new log lines, buffers and flushes them if max lines limit exceeded.
    ///
    /// Returns `true` if all the senders have been dropped and the task should no
    /// longer wait for new messages and finish.
    async fn receive_log_line(&mut self) -> bool {
        match self.receiver.recv().await {
            Some(SenderRequest::Flush(label)) => {
                if let Some((_last_logged, mut log_lines)) = self.buffer_map.remove(&label) {
                    Self::flush_by_label(&label, &mut log_lines);
                }
                false
            }
            Some(SenderRequest::Message(label, log_line)) => {
                match self.buffer_map.entry(label.clone()) {
                    Entry::Occupied(mut occupied) => {
                        let (last_logged, log_lines) = occupied.get_mut();
                        if log_lines.len() + 1 > self.max_buffered_lines {
                            // reached limit for this label,
                            // flush all buffered and new log line
                            Self::flush_by_label(&label, log_lines);
                            Self::log_by_level(&label, &log_line);
                        } else {
                            // below limit, push to buffer to flush later
                            log_lines.push(log_line);
                            *last_logged = Instant::now();
                        }
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert((Instant::now(), vec![log_line]));
                    }
                }
                false
            }
            Some(SenderRequest::Close) => {
                self.receiver.close();
                self.flush_all_buffered();
                true
            }
            None => {
                // no more senders, all LogLineSenders and LogLineSenderBuilder have been dropped
                self.flush_all_buffered();
                true
            }
        }
    }

    /// Flush all currently buffered contents without ordering, but grouped by label
    fn flush_all_buffered(&mut self) {
        for (label, (_last_logged, log_lines)) in self.buffer_map.iter_mut() {
            Self::flush_by_label(label, log_lines);
        }
    }

    /// Flush all currently buffered contents without ordering, but grouped by label
    fn flush_by_label(label: &str, log_lines: &mut Vec<LogLine>) {
        for log_line in log_lines.iter() {
            Self::log_by_level(label, log_line);
        }
        log_lines.clear();
    }

    /// Write the given log line prefixed by label
    fn log_by_level(label: &str, log_line: &LogLine) {
        match log_line.level {
            Level::ERROR => error!("[{label}]: {}", log_line.message),
            Level::WARN => warn!("[{label}]: {}", log_line.message),
            Level::INFO => info!("[{label}]: {}", log_line.message),
            Level::DEBUG => debug!("[{label}]: {}", log_line.message),
            Level::TRACE => trace!("[{label}]: {}", log_line.message),
        }
    }
}
