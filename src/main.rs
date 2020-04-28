// s3rename expr s3://bucket/key-prefix
// -n, --dry-run - only print changes
// -q, --quiet - do not print key modifications
// -i, --interactive - ask for overwrite (TODO later)
// -o, --no-overwrite - do not overwrite keys if they exist (need to list target keys)
// -v, --verbose - print debug messages
#[macro_use]
extern crate lazy_static;

mod args;

use anyhow::{anyhow, Result};
use args::ArgumentError;
use core::str::FromStr;
use rusoto_core::Region;
use rusoto_s3::GetBucketLocationRequest;
use rusoto_s3::{S3Client, S3};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let opt = args::App::from_args();
    println!("{:?}", opt);
    let client = S3Client::new(opt.aws_region.clone().unwrap_or(Region::UsEast1)); //TODO: Try to get region from AWS config too

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

    let target_region = match (opt.aws_region, bucket_region) {
        (Some(aws_region), _) => Ok(aws_region),
        (None, Some(bucket_region)) => Ok(bucket_region),
        (None, None) => Err(ArgumentError::CouldNotDetermineBucketRegion {
            bucket: opt.s3_url.bucket,
        }),
    }?;

    println!("{:?}", target_region);

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
