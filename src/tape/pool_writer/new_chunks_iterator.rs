use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use anyhow::{format_err, Error};

use pbs_datastore::{DataBlob, DataStore, SnapshotReader};

use crate::tape::CatalogSet;
use crate::tools::parallel_handler::ParallelHandler;

/// Chunk iterator which uses separate threads to read chunks
///
/// The iterator skips duplicate chunks and chunks already in the
/// catalog.
pub struct NewChunksIterator {
    #[allow(clippy::type_complexity)]
    rx: std::sync::mpsc::Receiver<Result<Option<([u8; 32], DataBlob)>, Error>>,
}

impl NewChunksIterator {
    /// Creates the iterator, spawning a new thread
    ///
    /// Make sure to join() the returned thread handle.
    pub fn spawn(
        datastore: Arc<DataStore>,
        snapshot_reader: Arc<Mutex<SnapshotReader>>,
        catalog_set: Arc<Mutex<CatalogSet>>,
        read_threads: usize,
    ) -> Result<(std::thread::JoinHandle<()>, Self), Error> {
        // set the buffer size of the channel queues to twice the number of threads or 3, whichever
        // is greater, to reduce the chance of a reader thread (producer) being blocked.
        let (tx, rx) = std::sync::mpsc::sync_channel((read_threads * 2).max(3));

        let reader_thread = std::thread::spawn(move || {
            let snapshot_reader = snapshot_reader.lock().unwrap();

            let mut chunk_index: HashSet<[u8; 32]> = HashSet::new();

            let datastore_name = snapshot_reader.datastore_name().to_string();

            let result: Result<(), Error> = proxmox_lang::try_block!({
                let chunk_iter = snapshot_reader.chunk_iterator(move |digest| {
                    catalog_set
                        .lock()
                        .unwrap()
                        .contains_chunk(&datastore_name, digest)
                })?;

                let reader_pool =
                    ParallelHandler::new("tape backup chunk reader pool", read_threads, {
                        let tx = tx.clone();
                        move |digest| {
                            let blob = datastore.load_chunk(&digest)?;

                            tx.send(Ok(Some((digest, blob)))).map_err(|err| {
                                format_err!("error sending result from reader thread: {err}")
                            })?;

                            Ok(())
                        }
                    });

                for digest in chunk_iter {
                    let digest = digest?;

                    if chunk_index.contains(&digest) {
                        continue;
                    }

                    reader_pool.send(digest)?;

                    chunk_index.insert(digest);
                }

                reader_pool.complete()?;

                let _ = tx.send(Ok(None)); // ignore send error

                Ok(())
            });
            if let Err(err) = result {
                if let Err(err) = tx.send(Err(err)) {
                    eprintln!("error sending result to reader thread: {err}");
                }
            }
        });

        Ok((reader_thread, Self { rx }))
    }
}

// We do not use Receiver::into_iter(). The manual implementation
// returns a simpler type.
impl Iterator for NewChunksIterator {
    type Item = Result<([u8; 32], DataBlob), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.rx.recv() {
            Ok(Ok(None)) => None,
            Ok(Ok(Some((digest, blob)))) => Some(Ok((digest, blob))),
            Ok(Err(err)) => Some(Err(err)),
            Err(_) => Some(Err(format_err!("reader thread failed"))),
        }
    }
}
