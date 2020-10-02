use log::{debug, error};
use rusoto_s3::{CopyObjectRequest, DeleteObjectRequest};
use rusoto_s3::{S3Client, S3};
use std::sync::{Arc, Mutex};
pub struct WrappedCopyRequest {
    bucket: String,
    src_key: String,
    client: Arc<S3Client>,
    destructor_futures: Arc<Mutex<futures::stream::FuturesUnordered<tokio::task::JoinHandle<()>>>>,
}

impl WrappedCopyRequest {
    pub async fn new(
        client: Arc<S3Client>,
        request: CopyObjectRequest,
        src_key: String,
        destructor_futures: Arc<
            Mutex<futures::stream::FuturesUnordered<tokio::task::JoinHandle<()>>>,
        >,
    ) -> Result<Self, anyhow::Error> {
        let bucket = request.bucket.clone();
        match client.copy_object(request).await {
            Ok(_) => Ok(WrappedCopyRequest {
                bucket,
                src_key,
                client,
                destructor_futures,
            }),
            Err(x) => Err(anyhow::Error::from(x)),
        }
    }
}

impl Drop for WrappedCopyRequest {
    fn drop(&mut self) {
        let delete_request = DeleteObjectRequest {
            bucket: self.bucket.clone(),
            bypass_governance_retention: None, // TODO: For Object Lock
            key: self.src_key.clone(),
            mfa: None, // TODO: Required to permanently delete if MFA and versioning enabled
            request_payer: None,
            version_id: None,
        };

        // TODO: Can we avoid this clone - only used for debugging in match below
        let key = delete_request.key.clone();
        debug!("Dropping key: {}", key);

        // use spawn so we don't block
        // need reference to client
        // write handles to a FuturesUnordered - can we avoid Mutex here?
        let move_client: Arc<S3Client> = self.client.clone();

        let handle = tokio::spawn(async move {
            match move_client.delete_object(delete_request).await {
                Ok(_) => {
                    debug!("Deleted {}", key);
                }
                Err(x) => {
                    error!("{:?}", x);
                }
            }
        });
        self.destructor_futures.lock().unwrap().push(handle);
    }
}
