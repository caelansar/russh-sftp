use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures_core::Stream;
use tokio::runtime::Handle;

use super::Metadata;
use crate::client::error::Error;
use crate::client::rawsession::SftpResult;
use crate::client::RawSftpSession;
use crate::protocol::{FileType, Name, StatusCode};

/// Entries returned by the [`ReadDir`] iterator and [`ReadDirStream`].
#[derive(Debug)]
pub struct DirEntry {
    file: String,
    metadata: Metadata,
}

impl DirEntry {
    /// Returns the file name for the file that this entry points at.
    pub fn file_name(&self) -> String {
        self.file.to_owned()
    }

    /// Returns the file type for the file that this entry points at.
    pub fn file_type(&self) -> FileType {
        self.metadata.file_type()
    }

    /// Returns the metadata for the file that this entry points at.
    pub fn metadata(&self) -> Metadata {
        self.metadata.to_owned()
    }
}

/// Iterator over the entries in a remote directory.
pub struct ReadDir {
    pub(crate) entries: VecDeque<(String, Metadata)>,
}

impl Iterator for ReadDir {
    type Item = DirEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match self.entries.pop_front() {
            None => None,
            Some(entry) if entry.0 == "." || entry.0 == ".." => self.next(),
            Some(entry) => Some(DirEntry {
                file: entry.0,
                metadata: entry.1,
            }),
        }
    }
}

/// A stream over the entries in a remote directory.
///
/// Unlike [`ReadDir`], which buffers all entries before returning, this stream
/// yields entries lazily as they are fetched from the server in batches via
/// `SSH_FXP_READDIR` requests.
///
/// The directory handle is automatically closed when the stream reaches EOF or
/// is dropped. If you need to guarantee that the handle is closed before
/// proceeding, use the [`close`](ReadDirStream::close) method.
pub struct ReadDirStream {
    session: Arc<RawSftpSession>,
    handle: String,
    buffer: VecDeque<(String, Metadata)>,
    future: Option<Pin<Box<dyn Future<Output = SftpResult<Name>> + Send>>>,
    done: bool,
    closed: bool,
}

impl ReadDirStream {
    pub(crate) fn new(session: Arc<RawSftpSession>, handle: String) -> Self {
        Self {
            session,
            handle,
            buffer: VecDeque::new(),
            future: None,
            done: false,
            closed: false,
        }
    }

    /// Explicitly closes the directory handle.
    ///
    /// This is not strictly necessary since the handle is closed automatically
    /// on drop, but it allows you to handle close errors and ensures the handle
    /// is closed before proceeding.
    pub async fn close(mut self) -> SftpResult<()> {
        self.closed = true;
        self.session.close(self.handle.clone()).await.map(|_| ())
    }
}

impl Stream for ReadDirStream {
    type Item = SftpResult<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        loop {
            // 1. Drain buffered entries first, skipping "." and ".."
            if let Some(entry) = this.buffer.pop_front() {
                if entry.0 == "." || entry.0 == ".." {
                    continue;
                }
                return Poll::Ready(Some(Ok(DirEntry {
                    file: entry.0,
                    metadata: entry.1,
                })));
            }

            // 2. If we've reached EOF, nothing more to yield
            if this.done {
                return Poll::Ready(None);
            }

            // 3. Create the readdir future if we don't have one in-flight
            let future = this.future.get_or_insert_with(|| {
                let session = this.session.clone();
                let handle = this.handle.clone();
                Box::pin(async move { session.readdir(handle).await })
            });

            // 4. Poll the in-flight future
            match future.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(name)) => {
                    this.future = None;
                    this.buffer
                        .extend(name.files.into_iter().map(|f| (f.filename, f.attrs)));
                    // Loop back to drain the buffer
                }
                Poll::Ready(Err(Error::Status(status)))
                    if status.status_code == StatusCode::Eof =>
                {
                    this.future = None;
                    this.done = true;

                    // Close the handle inline since we've reached EOF
                    let session = this.session.clone();
                    let dir_handle = this.handle.clone();
                    this.closed = true;
                    this.future = None;

                    // Spawn close as a background task — we don't block the stream on it
                    if let Ok(rt_handle) = Handle::try_current() {
                        rt_handle.spawn(async move {
                            let _ = session.close(dir_handle).await;
                        });
                    }

                    return Poll::Ready(None);
                }
                Poll::Ready(Err(err)) => {
                    this.future = None;
                    return Poll::Ready(Some(Err(err)));
                }
            }
        }
    }
}

impl Drop for ReadDirStream {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        if let Ok(handle) = Handle::try_current() {
            let session = self.session.clone();
            let dir_handle = self.handle.clone();

            handle.spawn(async move {
                let _ = session.close(dir_handle).await;
            });
        }
    }
}
