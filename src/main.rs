// s3rename expr s3://bucket/key-prefix
// -n, --dry-run - only print changes
// -q, --quiet - do not print key modifications
// -i, --interactive - ask for overwrite (TODO later)
// -o, --no-overwrite - do not overwrite keys if they exist (need to list target keys)
// -v, --verbose - print debug messages
//
// TODO: Debug logging + verbose
// TODO: Date modified filters
// TODO: Encryption, ACL override, checksum checking
// TODO: Allow choice between DeleteObject (as we copy) and DeleteObjects in bulk
#[macro_use]
extern crate lazy_static;

mod args;

use anyhow::{anyhow, Result};
use args::ArgumentError;
use core::str::FromStr;
use futures::stream::StreamExt;
use rusoto_core::Region;
use rusoto_s3::{CopyObjectRequest, DeleteObjectsRequest};
use rusoto_s3::{GetBucketLocationRequest, ListObjectsV2Request};
use rusoto_s3::{S3Client, S3};
use sedregex::ReplaceCommand;
use structopt::StructOpt;
use tokio::prelude::*;
// TODO: Move me
use thiserror::Error;
#[derive(Error, Debug)]
pub enum S3Error {
    #[error("Bucket is empty, or no matching prefixes: s3://{bucket}/{prefix}")]
    EmptyBucket { bucket: String, prefix: String },
}

#[derive(Error, Debug)]
pub enum ExpressionError {
    #[error("Could not parse expression: {expression}, error: {error:?}")]
    SedRegexParseError {
        expression: String,
        error: sedregex::ErrorKind,
    },
}
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let opt = args::App::from_args();
    println!("{:?}", opt);
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
        Some(Err(err)) => None, // TODO: Return error here
        Some(Ok(aws_region)) => Some(aws_region),
    };

    let target_region = match (opt.aws_region.clone(), bucket_region) {
        (Some(aws_region), _) => Ok(aws_region),
        (None, Some(bucket_region)) => Ok(bucket_region),
        (None, None) => Err(ArgumentError::CouldNotDetermineBucketRegion {
            bucket: opt.s3_url.bucket.clone(), // TODO: try fallback to AWS config here?
        }),
    }?;

    dbg!(&target_region);
    let client = S3Client::new(target_region); //TODO: Try to get region from AWS config too - does Rusoto provide this?

    // TODO Loop checking if truncated here, for buckets with >10k files
    let objects_inner = match client
        .list_objects_v2(ListObjectsV2Request {
            bucket: opt.s3_url.bucket.clone(), //TODO: fix clone?
            continuation_token: None,
            delimiter: None,
            encoding_type: None,
            fetch_owner: None,
            max_keys: None,
            prefix: opt.s3_url.key_prefix.clone(),
            request_payer: None,
            start_after: None,
        })
        .await?
        .contents
    {
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

    let inner_keys: Vec<String> = objects_inner
        .into_iter()
        .filter(|x| x.key.is_some())
        .map(|x| x.key.unwrap())
        .collect();

    println!("{:?}", inner_keys);
    let replace_command = match ReplaceCommand::new(&opt.expr) {
        Ok(x) => Ok(x),
        Err(err) => Err(ExpressionError::SedRegexParseError {
            expression: opt.expr.clone(),
            error: err,
        }),
    }?;

    // TODO Async
    let mut futures: futures::stream::FuturesUnordered<_> = inner_keys
        .iter()
        .map(|x| {
            handle_key(
                &client,
                &opt.s3_url.bucket,
                x,
                &replace_command,
                opt.dry_run,
                opt.quiet,
            )
        })
        .collect();

    // futures.then(|_| Ok(()));
    while let Some(handled) = futures.next().await {}

    // Naive approach:
    // List all keys in bucket with given prefix
    // Apply regex to all keys
    // Filter for keys that have changed
    // Make PutObject requests for these keys
    //
    // Improvements:
    // Can we filter given pattern (i.e. if the replace pattern has a prefix, append that to existing
    // prefix?
    //
    // Set up thread pool with Tokio
    Ok(())
}

// TODO: Thread safe, key could be moved - replace_command should be shared
async fn handle_key(
    client: &S3Client,
    bucket: &str,
    key: &str,
    replace_command: &ReplaceCommand<'_>,
    dry_run: bool,
    quiet: bool,
) -> Result<(), anyhow::Error> {
    // TODO move allocation outside, move in closure
    let newkey = replace_command.execute(key);
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

    match client.copy_object(copy_request).await {
        Ok(_) => Ok(()),
        Err(x) => Err(anyhow::Error::from(x)),
    }
}
