use std::cmp;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};

use anyhow::{anyhow, Error};
use futures::{ready, Stream};
use pin_project_lite::pin_project;

use crate::backup_stats::UploadCounters;
use crate::pxar::create::ReusableDynamicEntry;

pin_project! {
    pub struct InjectReusedChunksQueue<S> {
        #[pin]
        input: S,
        next_injection: Option<InjectChunks>,
        injections: Option<mpsc::Receiver<InjectChunks>>,
        counters: UploadCounters,
    }
}

type StreamOffset = u64;
#[derive(Debug)]
/// Holds a list of chunks to inject at the given boundary by forcing a chunk boundary.
pub struct InjectChunks {
    /// Offset at which to force the boundary
    pub boundary: StreamOffset,
    /// List of chunks to inject
    pub chunks: Vec<ReusableDynamicEntry>,
    /// Cumulative size of the chunks in the list
    pub size: usize,
}

/// Variants for stream consumer to distinguish between raw data chunks and injected ones.
pub enum InjectedChunksInfo {
    Known(Vec<ReusableDynamicEntry>),
    Raw(bytes::BytesMut),
}

pub trait InjectReusedChunks: Sized {
    fn inject_reused_chunks(
        self,
        injections: Option<mpsc::Receiver<InjectChunks>>,
        counters: UploadCounters,
    ) -> InjectReusedChunksQueue<Self>;
}

impl<S> InjectReusedChunks for S
where
    S: Stream<Item = Result<bytes::BytesMut, Error>>,
{
    fn inject_reused_chunks(
        self,
        injections: Option<mpsc::Receiver<InjectChunks>>,
        counters: UploadCounters,
    ) -> InjectReusedChunksQueue<Self> {
        InjectReusedChunksQueue {
            input: self,
            next_injection: None,
            injections,
            counters,
        }
    }
}

impl<S> Stream for InjectReusedChunksQueue<S>
where
    S: Stream<Item = Result<bytes::BytesMut, Error>>,
{
    type Item = Result<InjectedChunksInfo, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        // loop to skip over possible empty chunks
        loop {
            if this.next_injection.is_none() {
                if let Some(injections) = this.injections.as_mut() {
                    if let Ok(injection) = injections.try_recv() {
                        *this.next_injection = Some(injection);
                    }
                }
            }

            if let Some(inject) = this.next_injection.take() {
                // got reusable dynamic entries to inject
                let offset = this.counters.total_stream_len() as u64;

                match inject.boundary.cmp(&offset) {
                    // inject now
                    cmp::Ordering::Equal => {
                        let chunk_info = InjectedChunksInfo::Known(inject.chunks);
                        return Poll::Ready(Some(Ok(chunk_info)));
                    }
                    // inject later
                    cmp::Ordering::Greater => *this.next_injection = Some(inject),
                    // incoming new chunks and injections didn't line up?
                    cmp::Ordering::Less => {
                        return Poll::Ready(Some(Err(anyhow!("invalid injection boundary"))))
                    }
                }
            }

            // nothing to inject now, await further input
            match ready!(this.input.as_mut().poll_next(cx)) {
                None => {
                    if let Some(injections) = this.injections.as_mut() {
                        if this.next_injection.is_some() || injections.try_recv().is_ok() {
                            // stream finished, but remaining dynamic entries to inject
                            return Poll::Ready(Some(Err(anyhow!(
                                "injection queue not fully consumed"
                            ))));
                        }
                    }
                    // stream finished and all dynamic entries already injected
                    return Poll::Ready(None);
                }
                Some(Err(err)) => return Poll::Ready(Some(Err(err))),
                // ignore empty chunks, injected chunks from queue at forced boundary, but boundary
                // did not require splitting of the raw stream buffer to force the boundary
                Some(Ok(raw)) if raw.is_empty() => continue,
                Some(Ok(raw)) => return Poll::Ready(Some(Ok(InjectedChunksInfo::Raw(raw)))),
            }
        }
    }
}
