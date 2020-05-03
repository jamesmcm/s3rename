use super::errors::ArgumentError;
use core::str::FromStr;
use num_derive::FromPrimitive;
use num_traits::FromPrimitive;
use regex::Regex;
use sedregex::ReplaceCommand;
use structopt::StructOpt;

#[derive(Debug, FromPrimitive, Clone, Copy)]
pub enum CannedACL {
    Private,
    PublicRead,
    PublicReadWrite,
    AWSExecRead,
    AuthenticatedRead,
    BucketOwnerRead,
    BucketOwnerFullControl,
}

impl CannedACL {
    pub fn possible_strings() -> &'static [&'static str] {
        &[
            "private",
            "public-read",
            "public-read-write",
            "aws-exec-read",
            "authenticated-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ][..]
    }
}

impl FromStr for CannedACL {
    type Err = ArgumentError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        for (i, t) in CannedACL::possible_strings().iter().enumerate() {
            if *t == s {
                return Ok(FromPrimitive::from_usize(i).unwrap());
            }
        }
        Err(Self::Err::InvalidCannedACL {
            s: String::from(s),
            possible_strings: CannedACL::possible_strings(),
        })
    }
}

impl ToString for CannedACL {
    fn to_string(&self) -> String {
        String::from(CannedACL::possible_strings()[*self as usize])
    }
}

fn parse_s3_prefix_url(src: &str) -> Result<S3Prefix, ArgumentError> {
    lazy_static! {
        static ref S3_REGEX: Regex =
            Regex::new(r"s3://([A-Za-z0-9_\-.]+)/?([@&:,$=+?;#A-Za-z0-9_\-/!.*'()%\s{}\[\]]+)?")
                .unwrap();
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

    /// Do not preserve object properties (saves retrieving per-object details) - using this flag
    /// will remove any encryption (does not affect ACL)
    #[structopt(long)]
    pub no_preserve_properties: bool,

    /// Perl RegEx Replace Expression (only s/target/replacement/flags form supported)
    #[structopt(parse(try_from_str = replace_command_from_str))]
    pub expr: String,

    /// S3 URL: s3://bucket-name/optional-key-prefix
    #[structopt(parse(try_from_str = parse_s3_prefix_url))]
    pub s3_url: S3Prefix,

    /// AWS Region (will be taken from bucket region if not overridden here)
    #[structopt(long, parse(try_from_str = rusoto_core::Region::from_str))]
    pub aws_region: Option<rusoto_core::Region>,

    /// Canned access_control_list override - sets this ACL for all renamed keys
    #[structopt(long, possible_values = CannedACL::possible_strings(), parse(try_from_str = CannedACL::from_str))]
    pub canned_acl: Option<CannedACL>,

    /// Do not preserve Object ACL settings (set all to private)
    #[structopt(long)]
    pub no_preserve_acl: bool,
}
