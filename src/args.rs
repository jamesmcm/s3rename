use regex::Regex;
use sedregex::ReplaceCommand;
use structopt::StructOpt;
use thiserror::Error;

use core::str::FromStr;
#[derive(Error, Debug)]
pub enum ArgumentError {
    #[error("Invalid S3 URL: {url:?}, expected format: s3://bucket/optional-key-prefix")]
    InvalidS3Url { url: String },
    #[error("Could not determine bucket region for S3 bucket: s3://{bucket:?}, please specify with --aws-region")]
    CouldNotDetermineBucketRegion { bucket: String },
}

fn parse_s3_prefix_url(src: &str) -> Result<S3Prefix, ArgumentError> {
    lazy_static! {
        static ref S3_REGEX: Regex =
            Regex::new(r"s3://([A-Za-z0-9_-]+)/?([A-Za-z0-9_-]+)?").unwrap();
    }

    let captures = S3_REGEX.captures(src).ok_or(ArgumentError::InvalidS3Url {
        url: String::from(src),
    })?;

    Ok(S3Prefix {
        bucket: String::from(
            captures
                .get(1)
                .ok_or(ArgumentError::InvalidS3Url {
                    url: String::from(src),
                })?
                .as_str(),
        ),
        key_prefix: captures.get(2).map(|x| String::from(x.as_str())),
    })
}

#[derive(Debug)]
pub struct S3Prefix {
    // TODO: Wrap String to only take S3 acceptable chars (unicode?)
    pub bucket: String,
    pub key_prefix: Option<String>,
}

fn replace_command_from_str(s: &str) -> Result<String, sedregex::ErrorKind> {
    let r = ReplaceCommand::new(&s);
    r.map(|_| String::from(s))
}

#[derive(StructOpt, Debug)]
#[structopt(
    name = "s3rename",
    about = "Rename keys on S3 with Perl regular expressions"
)]
pub struct App {
    /// Print debug messages
    #[structopt(short, long)]
    pub verbose: bool,

    /// Do not print key modifications
    #[structopt(short, long)]
    pub quiet: bool,

    /// Do not carry out modifications (only print)
    #[structopt(short = "n", long)]
    pub dry_run: bool,

    /// Perl RegEx Replace Expression (only s/target/replacement/flags form supported)
    #[structopt(parse(try_from_str = replace_command_from_str))]
    pub expr: String,

    /// S3 URL: s3://bucket-name/optional-key-prefix
    #[structopt(parse(try_from_str = parse_s3_prefix_url))]
    pub s3_url: S3Prefix,

    /// AWS Region (will be taken from bucket region if not overridden here)
    #[structopt(long, parse(try_from_str = rusoto_core::Region::from_str))]
    pub aws_region: Option<rusoto_core::Region>,
}
