#[macro_use]
extern crate lazy_static;
mod args;
mod errors;
mod wrapped_copy;

use std::sync::Arc;
use std::sync::Mutex;

use anyhow::Result;
use args::CannedACL;
use core::str::FromStr;
use errors::{ArgumentError, GranteeParseError};
use errors::{ExpressionError, S3Error};
use futures::stream::StreamExt;
use rusoto_core::Region;
use rusoto_s3::{CopyObjectRequest, GetObjectAclRequest, HeadObjectRequest};
use rusoto_s3::{GetBucketLocationRequest, ListObjectsV2Request};
use rusoto_s3::{Grantee, S3Client, S3};
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
            bucket: opt.s3_url.bucket.clone(),
        })
        .await?
        .location_constraint
        .map(|x| Region::from_str(&x))
    {
        None => None,
        Some(Err(_err)) => None, // Note we ignore failure to get bucket region
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
    let client = Arc::new(S3Client::new(target_region));

    // Collect all keys under prefix to this Vec (can we avoid this allocation)?
    let mut keys_vec = Vec::new(); // Can we use metadata request to estimate size here?
    let mut continuation_token = None;

    loop {
        // Here we loop until we are told that the request was not truncated (i.e. we have seen all
        // keys)
        let response = client
            .list_objects_v2(ListObjectsV2Request {
                bucket: opt.s3_url.bucket.clone(),
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
            // Note we return an error on no matching keys, may want to succeed silently
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
            .into_iter()
            .filter(|x| x.key.is_some())
            .map(|x| (x.key.unwrap(), x.storage_class))
            .filter(|x| x.0.chars().last().unwrap() != '/'); // Skip "directory" keys - TODO: check issues regarding empty directories

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

    // Used to store futures returned from destructors (so we do not terminate until destructors
    // have finished) - this pseudo-async destructor setup might violate atomicity (since a
    // terminate request will guarantee destructors run but not that the spawned async DeleteObject
    // requests finish) - TODO: Verify this and consider synchronous destructors
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
                opt.no_preserve_properties,
                opt.no_preserve_acl,
                opt.canned_acl,
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
    key: &(String, Option<String>),
    replace_command: &ReplaceCommand<'_>,
    dry_run: bool,
    quiet: bool, // TODO: Refactor these args in to a Copy struct
    verbose: bool,
    no_preserve_properties: bool,
    no_preserve_acl: bool,
    canned_acl: Option<CannedACL>,
    destructor_futures: Arc<Mutex<futures::stream::FuturesUnordered<tokio::task::JoinHandle<()>>>>,
) -> Result<(), anyhow::Error> {
    let newkey = replace_command.execute(&key.0);
    if newkey == key.0 {
        if verbose {
            println!("Skipping {:?} since key did not change", key);
        }
        return Ok(());
    }
    if !quiet {
        println!("Renaming {} to {}", key.0, newkey);
    }
    if dry_run {
        return Ok(());
    }

    let mut grant_read_vec: Vec<String> = Vec::new();
    let mut grant_read_acp_vec: Vec<String> = Vec::new();
    let mut grant_write_acp_vec: Vec<String> = Vec::new();
    let mut grant_full_control_vec: Vec<String> = Vec::new();

    if !no_preserve_acl && canned_acl.is_none() {
        let acl_request = GetObjectAclRequest {
            bucket: String::from(bucket),
            key: key.0.clone(),
            request_payer: None,
            version_id: None,
        };
        let acl_response = client.get_object_acl(acl_request).await?;
        if verbose {
            dbg!(&acl_response);
        }

        for grant in acl_response.grants.unwrap() {
            let _ok_check = match grant.permission.as_deref() {
                Some("READ") => {
                    let grantee = grant.grantee.unwrap();
                    grant_read_vec.push(generate_permission_grant(grantee)?);
                    Ok(())
                }
                Some("WRITE") => {
                    //TODO: No WRITE grant on CopyObjectRequest - is this controlled by bucket ACL?
                    if verbose {
                        println!(
                            "Warning: WRITE access ignored for grantee: {:?} on key: {}",
                            grant.grantee.unwrap(),
                            &key.0
                        );
                    }
                    Ok(())
                }
                Some("READ_ACP") => {
                    let grantee = grant.grantee.unwrap();
                    grant_read_acp_vec.push(generate_permission_grant(grantee)?);
                    Ok(())
                }
                Some("WRITE_ACP") => {
                    let grantee = grant.grantee.unwrap();
                    grant_write_acp_vec.push(generate_permission_grant(grantee)?);
                    Ok(())
                }
                Some("FULL_CONTROL") => {
                    let grantee = grant.grantee.unwrap();
                    grant_full_control_vec.push(generate_permission_grant(grantee)?);
                    Ok(())
                }
                Some(other) => Err(GranteeParseError::InvalidPermission {
                    permission: String::from(other),
                    grantee: grant.grantee.unwrap(),
                }),
                None => Err(GranteeParseError::MissingPermission {
                    grantee: grant.grantee.unwrap(),
                }),
            }?;
        }
    }
    let copy_request = match no_preserve_properties {
        false => {
            let head_request = HeadObjectRequest {
                bucket: String::from(bucket),
                if_match: None,
                if_modified_since: None,
                if_none_match: None,
                if_unmodified_since: None,
                key: key.0.clone(),
                part_number: None,
                range: None,
                request_payer: None,
                sse_customer_algorithm: None, // Seems we can get metadata for Copy without this
                sse_customer_key: None,
                sse_customer_key_md5: None,
                version_id: None,
            };
            let head_result = client.head_object(head_request).await?;
            CopyObjectRequest {
                acl: canned_acl.map(|x| x.to_string()),
                bucket: String::from(bucket),
                cache_control: head_result.cache_control,
                content_disposition: head_result.content_disposition,
                content_encoding: head_result.content_encoding,
                content_language: head_result.content_language,
                content_type: head_result.content_type,
                copy_source: format!("{}/{}", bucket, key.0),
                copy_source_if_match: None,
                copy_source_if_modified_since: None,
                copy_source_if_none_match: None,
                copy_source_if_unmodified_since: None,
                copy_source_sse_customer_algorithm: head_result.sse_customer_algorithm.clone(),
                copy_source_sse_customer_key: None, //TODO
                copy_source_sse_customer_key_md5: head_result.sse_customer_key_md5.clone(),
                expires: head_result.expires,
                grant_full_control: if grant_full_control_vec.len() > 0 {
                    Some(grant_full_control_vec.join(", "))
                } else {
                    None
                },
                grant_read: if grant_read_vec.len() > 0 {
                    Some(grant_read_vec.join(", "))
                } else {
                    None
                },
                grant_read_acp: if grant_read_acp_vec.len() > 0 {
                    Some(grant_read_acp_vec.join(", "))
                } else {
                    None
                },
                grant_write_acp: if grant_write_acp_vec.len() > 0 {
                    Some(grant_write_acp_vec.join(", "))
                } else {
                    None
                },
                key: String::from(newkey.to_owned()),
                metadata: head_result.metadata,
                metadata_directive: Some(String::from("REPLACE")), // Set to REPLACE due to
                // multi-part copies: https://docs.aws.amazon.com/cli/latest/reference/s3/cp.html
                object_lock_legal_hold_status: head_result.object_lock_legal_hold_status,
                object_lock_mode: head_result.object_lock_mode,
                object_lock_retain_until_date: head_result.object_lock_retain_until_date,
                request_payer: head_result.request_charged, // TODO: Test me
                sse_customer_algorithm: head_result.sse_customer_algorithm.clone(),
                sse_customer_key: None, // TODO
                sse_customer_key_md5: head_result.sse_customer_key_md5.clone(),
                ssekms_encryption_context: None, // TODO
                ssekms_key_id: head_result.ssekms_key_id,
                server_side_encryption: head_result.server_side_encryption,
                storage_class: key.1.clone(),
                tagging: None, // tagging_directive should cover this anyway
                tagging_directive: Some(String::from("COPY")),
                website_redirect_location: head_result.website_redirect_location,
            }
        }
        true => CopyObjectRequest {
            acl: canned_acl.map(|x| x.to_string()),
            bucket: String::from(bucket),
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_type: None,
            copy_source: format!("{}/{}", bucket, key.0),
            copy_source_if_match: None,
            copy_source_if_modified_since: None,
            copy_source_if_none_match: None,
            copy_source_if_unmodified_since: None,
            copy_source_sse_customer_algorithm: None,
            copy_source_sse_customer_key: None,
            copy_source_sse_customer_key_md5: None,
            expires: None,
            grant_full_control: if grant_full_control_vec.len() > 0 {
                Some(grant_full_control_vec.join(", "))
            } else {
                None
            },
            grant_read: if grant_read_vec.len() > 0 {
                Some(grant_read_vec.join(", "))
            } else {
                None
            },
            grant_read_acp: if grant_read_acp_vec.len() > 0 {
                Some(grant_read_acp_vec.join(", "))
            } else {
                None
            },
            grant_write_acp: if grant_write_acp_vec.len() > 0 {
                Some(grant_write_acp_vec.join(", "))
            } else {
                None
            },
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
            ssekms_key_id: None,
            server_side_encryption: None,
            storage_class: key.1.clone(),
            tagging: None,
            tagging_directive: Some(String::from("COPY")),
            website_redirect_location: None,
        },
    };

    let _copy_response: WrappedCopyRequest = WrappedCopyRequest::new(
        client.clone(),
        copy_request,
        key.0.clone(),
        verbose,
        destructor_futures.clone(),
    )
    .await?;

    Ok(())
}

/// Convert a Grantee object to a grant String to use in the CopyObjectRequest
fn generate_permission_grant(grantee: Grantee) -> Result<String, GranteeParseError> {
    if let Some(uri) = grantee.uri {
        return Ok(format!("uri=\"{}\"", uri));
    }
    if let Some(id) = grantee.id {
        return Ok(format!("id=\"{}\"", id));
    }
    if let Some(email) = grantee.email_address {
        return Ok(format!("emailAddress=\"{}\"", email));
    }
    Err(GranteeParseError::NoValidID { grantee })
}
