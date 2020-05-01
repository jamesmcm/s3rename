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
// TODO: Atomic CopyObject so we cannot end up with objects copied or data loss
// TODO: Verify it runs on multiple threads
#[macro_use]
extern crate lazy_static;

mod args;
mod errors;

use anyhow::Result;
use core::str::FromStr;
use errors::ArgumentError;
use errors::{ExpressionError, S3Error};
use futures::stream::StreamExt;
use rusoto_core::Region;
use rusoto_s3::{CopyObjectRequest, DeleteObjectRequest}; // TODO: DeleteObjectsRequest
use rusoto_s3::{GetBucketLocationRequest, ListObjectsV2Request};
use rusoto_s3::{S3Client, S3};
use sedregex::ReplaceCommand;
use structopt::StructOpt;

// TODO: Move me
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
    let client = S3Client::new(target_region); //TODO: Try to get region from AWS config too - does Rusoto provide this?

    // TODO Loop checking if truncated here, for buckets with >10k files
    let objects_inner = match client
        .list_objects_v2(ListObjectsV2Request {
            bucket: opt.s3_url.bucket.clone(), //TODO: fix clone in Rusoto requests?
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

    if opt.verbose {
        dbg!("{:?}", &inner_keys);
    }
    let replace_command = match ReplaceCommand::new(&opt.expr) {
        Ok(x) => Ok(x),
        Err(err) => Err(ExpressionError::SedRegexParseError {
            expression: opt.expr.clone(),
            error: err,
        }),
    }?;

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

    while let Some(_handled) = futures.next().await {}

    Ok(())
}

// TODO: Does this actually work on multiple threads?
async fn handle_key(
    client: &S3Client,
    bucket: &str,
    key: &str,
    replace_command: &ReplaceCommand<'_>,
    dry_run: bool,
    quiet: bool,
) -> Result<(), anyhow::Error> {
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

    let _copy_response = match client.copy_object(copy_request).await {
        Ok(_) => Ok(()),
        Err(x) => Err(anyhow::Error::from(x)),
    }?;

    let delete_request = DeleteObjectRequest {
        bucket: String::from(bucket),
        bypass_governance_retention: None,
        key: String::from(key),
        mfa: None, // TODO: Required to delete if MFA and versioning enabled
        request_payer: None,
        version_id: None,
    };

    match client.delete_object(delete_request).await {
        Ok(_) => Ok(()),
        Err(x) => Err(anyhow::Error::from(x)),
    }
}
