use std::io::Write;
//use std::os::unix::io::FromRawFd;
use std::path::Path;
use std::pin::Pin;
use std::sync::{mpsc, Arc, Mutex};
use std::task::{Context, Poll};

use anyhow::Error;
use futures::future::{AbortHandle, Abortable};
use futures::stream::Stream;
use nix::dir::Dir;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;

use proxmox_async::blocking::TokioWriterAdapter;
use proxmox_io::StdChannelWriter;
use proxmox_log::debug;

use pbs_datastore::catalog::{BackupCatalogWriter, CatalogWriter};

use crate::inject_reused_chunks::InjectChunks;
use crate::pxar::create::PxarWriters;

/// Stream implementation to encode and upload .pxar archives.
///
/// The hyper client needs an async Stream for file upload, so we
/// spawn an extra thread to encode the .pxar data and pipe it to the
/// consumer.
pub struct PxarBackupStream {
    rx: Option<std::sync::mpsc::Receiver<Result<Vec<u8>, Error>>>,
    pub suggested_boundaries: Option<std::sync::mpsc::Receiver<u64>>,
    handle: Option<AbortHandle>,
    error: Arc<Mutex<Option<Error>>>,
}

impl Drop for PxarBackupStream {
    fn drop(&mut self) {
        self.rx = None;
        self.handle.take().unwrap().abort();
    }
}

impl PxarBackupStream {
    pub fn new<W: Write + Send + 'static>(
        dir: Dir,
        catalog: Option<Arc<Mutex<CatalogWriter<W>>>>,
        options: crate::pxar::PxarCreateOptions,
        boundaries: Option<mpsc::Sender<InjectChunks>>,
        separate_payload_stream: bool,
    ) -> Result<(Self, Option<Self>), Error> {
        let buffer_size = 256 * 1024;

        let (tx, rx) = std::sync::mpsc::sync_channel(10);
        let writer = TokioWriterAdapter::new(std::io::BufWriter::with_capacity(
            buffer_size,
            StdChannelWriter::new(tx),
        ));
        let writer = pxar::encoder::sync::StandardWriter::new(writer);

        let (writer, payload_rx, suggested_boundaries_tx, suggested_boundaries_rx) =
            if separate_payload_stream {
                let (tx, rx) = std::sync::mpsc::sync_channel(10);
                let (suggested_boundaries_tx, suggested_boundaries_rx) = std::sync::mpsc::channel();
                let payload_writer = TokioWriterAdapter::new(std::io::BufWriter::with_capacity(
                    buffer_size,
                    StdChannelWriter::new(tx),
                ));
                (
                    pxar::PxarVariant::Split(
                        writer,
                        pxar::encoder::sync::StandardWriter::new(payload_writer),
                    ),
                    Some(rx),
                    Some(suggested_boundaries_tx),
                    Some(suggested_boundaries_rx),
                )
            } else {
                (pxar::PxarVariant::Unified(writer), None, None, None)
            };

        let error = Arc::new(Mutex::new(None));
        let error2 = Arc::clone(&error);
        let handler = async move {
            if let Err(err) = crate::pxar::create_archive(
                dir,
                PxarWriters::new(
                    writer,
                    catalog.map(|c| c as Arc<Mutex<dyn BackupCatalogWriter + Send>>),
                ),
                crate::pxar::Flags::DEFAULT,
                move |path| {
                    debug!("{:?}", path);
                    Ok(())
                },
                options,
                boundaries,
                suggested_boundaries_tx,
            )
            .await
            {
                let mut error = error2.lock().unwrap();
                *error = Some(err);
            }
        };

        let (handle, registration) = AbortHandle::new_pair();
        let future = Abortable::new(handler, registration);
        tokio::spawn(future);

        let backup_stream = Self {
            rx: Some(rx),
            suggested_boundaries: None,
            handle: Some(handle.clone()),
            error: Arc::clone(&error),
        };

        let backup_payload_stream = payload_rx.map(|rx| Self {
            rx: Some(rx),
            suggested_boundaries: suggested_boundaries_rx,
            handle: Some(handle),
            error,
        });

        Ok((backup_stream, backup_payload_stream))
    }

    pub fn open<W: Write + Send + 'static>(
        dirname: &Path,
        catalog: Option<Arc<Mutex<CatalogWriter<W>>>>,
        options: crate::pxar::PxarCreateOptions,
        boundaries: Option<mpsc::Sender<InjectChunks>>,
        separate_payload_stream: bool,
    ) -> Result<(Self, Option<Self>), Error> {
        let dir = nix::dir::Dir::open(dirname, OFlag::O_DIRECTORY, Mode::empty())?;

        Self::new(dir, catalog, options, boundaries, separate_payload_stream)
    }
}

impl Stream for PxarBackupStream {
    type Item = Result<Vec<u8>, Error>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Option<Self::Item>> {
        {
            // limit lock scope
            let mut error = self.error.lock().unwrap();
            if let Some(err) = error.take() {
                return Poll::Ready(Some(Err(err)));
            }
        }

        match proxmox_async::runtime::block_in_place(|| self.rx.as_ref().unwrap().recv()) {
            Ok(data) => Poll::Ready(Some(data)),
            Err(_) => {
                let mut error = self.error.lock().unwrap();
                if let Some(err) = error.take() {
                    return Poll::Ready(Some(Err(err)));
                }
                Poll::Ready(None) // channel closed, no error
            }
        }
    }
}
