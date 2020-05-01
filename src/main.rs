// s3rename expr s3://bucket/key-prefix
// -n, --dry-run - only print changes
// -q, --quiet - do not print key modifications
// -i, --interactive - ask for overwrite (TODO later)
// -o, --no-overwrite - do not overwrite keys if they exist (need to list target keys) (TODO later)
// -v, --verbose - print debug messages
//
// TODO: Debug logging + verbose
// TODO: Date modified filters
// TODO: Encryption, ACL override, checksum checking
// TODO: Allow choice between DeleteObject (as we copy) and DeleteObjects in bulk
// TODO: Verify it runs on multiple threads
#[macro_use]
extern crate lazy_static;
mod args;
mod errors;
mod wrapped_copy;

use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Result;
use core::str::FromStr;
use errors::ArgumentError;
use errors::{ExpressionError, S3Error};
use futures::stream::StreamExt;
use rusoto_core::Region;
use rusoto_s3::CopyObjectRequest;
use rusoto_s3::{GetBucketLocationRequest, ListObjectsV2Request};
use rusoto_s3::{S3Client, S3};
use sedregex::ReplaceCommand;
use structopt::StructOpt;
use wrapped_copy::WrappedCopyRequest;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let opt = args::App::from_args();
    if opt.verbose {
        dbg!("{:?}", &opt);
    }
    let client = S3Client::new(opt.aws_region.clone().unwrap_or(Region::UsEast1)); //TODO: Try to get region from AWS config too - does Rusoto provide this?

    let bucket_region: Option<Region> = match client
        .get_bucket_location(GetBucketLocationRequest {
            bucket: opt.s3_url.bucket.clone(), //TODO: fix clone?
        })
        .await?
        .location_constraint
        .map(|x| Region::from_str(&x))
    {
        None => None,
        Some(Err(_err)) => None, // TODO: Maybe return error here? Rather than silently ignoring bucket region failure
        Some(Ok(aws_region)) => Some(aws_region),
    };

    let target_region = match (opt.aws_region.clone(), bucket_region) {
        (Some(aws_region), _) => Ok(aws_region),
        (None, Some(bucket_region)) => Ok(bucket_region),
        (None, None) => Err(ArgumentError::CouldNotDetermineBucketRegion {
            bucket: opt.s3_url.bucket.clone(), // TODO: try fallback to AWS config here?
        }),
    }?;

    if opt.verbose {
        dbg!(&target_region);
    }
    let client = Arc::new(S3Client::new(target_region)); //TODO: Try to get region from AWS config too - does Rusoto provide this?

    // Collect all keys under prefix to this Vec (can we avoid this allocation)?
    let mut keys_vec = Vec::new(); // Can we use metadata request to estimate size here?
    let mut continuation_token = None;

    loop {
        // Here we loop until we are told that the request was not truncated (i.e. we have seen all
        // keys)
        let response = client
            .list_objects_v2(ListObjectsV2Request {
                bucket: opt.s3_url.bucket.clone(), //TODO: fix clone in Rusoto requests?
                continuation_token,
                delimiter: None,
                encoding_type: None,
                fetch_owner: None,
                max_keys: None,
                prefix: opt.s3_url.key_prefix.clone(),
                request_payer: None,
                start_after: None,
            })
            .await?;

        // Set new continuation_token from response
        continuation_token = response.continuation_token.clone();

        let objects_inner = match response.contents {
            // TODO: Do we really want to return an error on no matching keys?
            None => Err(S3Error::EmptyBucket {
                bucket: opt.s3_url.bucket.clone(),
                prefix: if let Some(prefix) = opt.s3_url.key_prefix.clone() {
                    prefix
                } else {
                    String::new()
                },
            }),
            Some(x) => Ok(x),
        }?;

        // Get keys out of response
        let objects_inner = objects_inner
            .iter()
            .filter(|x| x.key.is_some())
            .map(|x| x.key.clone().unwrap());

        keys_vec.extend(objects_inner);

        // Break loop if keys were not truncated (i.e. no more keys)
        match response.is_truncated {
            Some(true) => {}
            _ => {
                break;
            }
        }
    }

    if opt.verbose {
        dbg!("{:?}", &keys_vec);
    }
    let replace_command = match ReplaceCommand::new(&opt.expr) {
        Ok(x) => Ok(x),
        Err(err) => Err(ExpressionError::SedRegexParseError {
            expression: opt.expr.clone(),
            error: err,
        }),
    }?;

    let destructor_futures = Arc::new(Mutex::new(futures::stream::FuturesUnordered::new()));

    let mut futures: futures::stream::FuturesUnordered<_> = keys_vec
        .iter()
        .map(|x| {
            handle_key(
                client.clone(),
                &opt.s3_url.bucket,
                x,
                &replace_command,
                opt.dry_run,
                opt.quiet,
                opt.verbose,
                destructor_futures.clone(),
            )
        })
        .collect();

    while let Some(_handled) = futures.next().await {}

    // Does Mutex make sense?
    while let Some(_handled) = destructor_futures.lock().unwrap().next().await {}

    Ok(())
}

// TODO: Does this actually work on multiple threads?
async fn handle_key(
    client: Arc<S3Client>,
    bucket: &str,
    key: &str,
    replace_command: &ReplaceCommand<'_>,
    dry_run: bool,
    quiet: bool, // TODO: Refactor these args in to a Copy struct
    verbose: bool,
    destructor_futures: Arc<Mutex<futures::stream::FuturesUnordered<tokio::task::JoinHandle<()>>>>,
) -> Result<(), anyhow::Error> {
    let newkey = replace_command.execute(key);
    if newkey == key {
        if verbose {
            println!("Skipping {} since key did not change", key);
        }
        return Ok(());
    }
    if !quiet {
        println!("Renaming {} to {}", key, newkey);
    }
    if dry_run {
        return Ok(());
    }
    let copy_request = CopyObjectRequest {
        acl: None,
        bucket: String::from(bucket),
        cache_control: None, //TODO
        content_disposition: None,
        content_encoding: None,
        content_language: None,
        content_type: None,
        copy_source: format!("{}/{}", bucket, key), //TODO URL Encoding
        copy_source_if_match: None,
        copy_source_if_modified_since: None,
        copy_source_if_none_match: None,
        copy_source_if_unmodified_since: None,
        copy_source_sse_customer_algorithm: None,
        copy_source_sse_customer_key: None,
        copy_source_sse_customer_key_md5: None,
        expires: None,
        grant_full_control: None,
        grant_read: None,
        grant_read_acp: None,
        grant_write_acp: None,
        key: String::from(newkey.to_owned()),
        metadata: None,
        metadata_directive: Some(String::from("COPY")),
        object_lock_legal_hold_status: None,
        object_lock_mode: None,
        object_lock_retain_until_date: None,
        request_payer: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
        ssekms_encryption_context: None,
        ssekms_key_id: None,          //TODO
        server_side_encryption: None, //TODO
        storage_class: None,          //TODO
        tagging: None,
        tagging_directive: None,
        website_redirect_location: None,
    };

    let _copy_response: WrappedCopyRequest = WrappedCopyRequest::new(
        client.clone(),
        copy_request,
        String::from(key),
        verbose,
        destructor_futures.clone(),
    )
    .await?;

    Ok(())
}
