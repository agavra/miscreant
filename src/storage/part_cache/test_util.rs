// Test-only helpers adapted from SlateDB `src/test_utils.rs` (Apache-2.0): the
// random-payload generator and an object store that stamps a marker extension
// on every response, used to assert extension pass-through.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use rand::Rng;

pub(crate) fn gen_rand_bytes(n: usize) -> Bytes {
    let mut rng = rand::rng();
    let random_bytes: Vec<u8> = (0..n).map(|_| rng.random::<u8>()).collect();
    Bytes::from(random_bytes)
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ExtensionMarker;

#[derive(Clone)]
pub(crate) struct ExtensionObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl ExtensionObjectStore {
    pub(crate) fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl fmt::Debug for ExtensionObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExtensionObjectStore({})", self.inner)
    }
}

impl fmt::Display for ExtensionObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ExtensionObjectStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for ExtensionObjectStore {
    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let mut result = self.inner.get_opts(location, options).await?;
        result.extensions.insert(ExtensionMarker);
        Ok(result)
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        let mut result = self.inner.put_opts(location, payload, opts).await?;
        result.extensions.insert(ExtensionMarker);
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}
