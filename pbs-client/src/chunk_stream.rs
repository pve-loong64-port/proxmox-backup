use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};

use anyhow::Error;
use bytes::BytesMut;
use futures::ready;
use futures::stream::{Stream, TryStream};

use pbs_datastore::{Chunker, ChunkerImpl};

use crate::inject_reused_chunks::InjectChunks;

/// Holds the queues for optional injection of reused dynamic index entries
pub struct InjectionData {
    boundaries: mpsc::Receiver<InjectChunks>,
    next_boundary: Option<InjectChunks>,
    injections: mpsc::Sender<InjectChunks>,
}

impl InjectionData {
    pub fn new(
        boundaries: mpsc::Receiver<InjectChunks>,
        injections: mpsc::Sender<InjectChunks>,
    ) -> Self {
        Self {
            boundaries,
            next_boundary: None,
            injections,
        }
    }
}

/// Split input stream into dynamic sized chunks
pub struct ChunkStream<S: Unpin> {
    input: S,
    chunker: Box<dyn Chunker + Send>,
    buffer: BytesMut,
    scan_pos: usize,
    consumed: u64,
    injection_data: Option<InjectionData>,
}

impl<S: Unpin> ChunkStream<S> {
    pub fn new(input: S, chunk_size: Option<usize>, injection_data: Option<InjectionData>) -> Self {
        let chunk_size = chunk_size.unwrap_or(4 * 1024 * 1024);
        Self {
            input,
            chunker: Box::new(ChunkerImpl::new(chunk_size)),
            buffer: BytesMut::new(),
            scan_pos: 0,
            consumed: 0,
            injection_data,
        }
    }
}

impl<S: Unpin> Unpin for ChunkStream<S> {}

impl<S: Unpin> Stream for ChunkStream<S>
where
    S: TryStream,
    S::Ok: AsRef<[u8]>,
    S::Error: Into<Error>,
{
    type Item = Result<BytesMut, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            let ctx = pbs_datastore::chunker::Context {
                base: this.consumed,
                total: this.buffer.len() as u64,
            };

            if let Some(InjectionData {
                boundaries,
                next_boundary,
                injections,
            }) = this.injection_data.as_mut()
            {
                if next_boundary.is_none() {
                    if let Ok(boundary) = boundaries.try_recv() {
                        *next_boundary = Some(boundary);
                    }
                }

                if let Some(inject) = next_boundary.take() {
                    // require forced boundary, lookup next regular boundary
                    let pos = if this.scan_pos < this.buffer.len() {
                        this.chunker.scan(&this.buffer[this.scan_pos..], &ctx)
                    } else {
                        0
                    };

                    let chunk_boundary = if pos == 0 {
                        this.consumed + this.buffer.len() as u64
                    } else {
                        this.consumed + (this.scan_pos + pos) as u64
                    };

                    if inject.boundary <= chunk_boundary {
                        // forced boundary is before next boundary, force within current buffer
                        let chunk_size = (inject.boundary - this.consumed) as usize;
                        let raw_chunk = this.buffer.split_to(chunk_size);
                        this.chunker.reset();
                        this.scan_pos = 0;

                        this.consumed += chunk_size as u64;

                        // add the size of the injected chunks to consumed, so chunk stream offsets
                        // are in sync with the rest of the archive.
                        this.consumed += inject.size as u64;

                        injections.send(inject).unwrap();

                        // the chunk can be empty, return nevertheless to allow the caller to
                        // make progress by consuming from the injection queue
                        return Poll::Ready(Some(Ok(raw_chunk)));
                    } else if pos != 0 {
                        *next_boundary = Some(inject);
                        // forced boundary is after next boundary, split off chunk from buffer
                        let chunk_size = this.scan_pos + pos;
                        let raw_chunk = this.buffer.split_to(chunk_size);
                        this.consumed += chunk_size as u64;
                        this.scan_pos = 0;

                        return Poll::Ready(Some(Ok(raw_chunk)));
                    } else {
                        // forced boundary is after current buffer length, continue reading
                        *next_boundary = Some(inject);
                        this.scan_pos = this.buffer.len();
                    }
                }
            }

            if this.scan_pos < this.buffer.len() {
                let boundary = this.chunker.scan(&this.buffer[this.scan_pos..], &ctx);

                let chunk_size = this.scan_pos + boundary;

                if boundary == 0 {
                    this.scan_pos = this.buffer.len();
                } else if chunk_size <= this.buffer.len() {
                    // found new chunk boundary inside buffer, split off chunk from buffer
                    let raw_chunk = this.buffer.split_to(chunk_size);
                    this.consumed += chunk_size as u64;
                    this.scan_pos = 0;
                    return Poll::Ready(Some(Ok(raw_chunk)));
                } else {
                    panic!("got unexpected chunk boundary from chunker");
                }
            }

            match ready!(Pin::new(&mut this.input).try_poll_next(cx)) {
                Some(Err(err)) => {
                    return Poll::Ready(Some(Err(err.into())));
                }
                None => {
                    this.scan_pos = 0;
                    if !this.buffer.is_empty() {
                        return Poll::Ready(Some(Ok(this.buffer.split())));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Some(Ok(data)) => {
                    this.buffer.extend_from_slice(data.as_ref());
                }
            }
        }
    }
}

/// Split input stream into fixed sized chunks
pub struct FixedChunkStream<S: Unpin> {
    input: S,
    chunk_size: usize,
    buffer: BytesMut,
}

impl<S: Unpin> FixedChunkStream<S> {
    pub fn new(input: S, chunk_size: usize) -> Self {
        Self {
            input,
            chunk_size,
            buffer: BytesMut::new(),
        }
    }
}

impl<S: Unpin> Unpin for FixedChunkStream<S> {}

impl<S: Unpin> Stream for FixedChunkStream<S>
where
    S: TryStream,
    S::Ok: AsRef<[u8]>,
{
    type Item = Result<BytesMut, S::Error>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> Poll<Option<Result<BytesMut, S::Error>>> {
        let this = self.get_mut();
        loop {
            if this.buffer.len() >= this.chunk_size {
                return Poll::Ready(Some(Ok(this.buffer.split_to(this.chunk_size))));
            }

            match ready!(Pin::new(&mut this.input).try_poll_next(cx)) {
                Some(Err(err)) => {
                    return Poll::Ready(Some(Err(err)));
                }
                None => {
                    // last chunk can have any size
                    if !this.buffer.is_empty() {
                        return Poll::Ready(Some(Ok(this.buffer.split())));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Some(Ok(data)) => {
                    this.buffer.extend_from_slice(data.as_ref());
                }
            }
        }
    }
}
