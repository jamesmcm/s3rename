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

#[derive(Error, Debug)]
pub enum ArgumentError {
    #[error("Invalid S3 URL: {url:?}, expected format: s3://bucket/optional-key-prefix")]
    InvalidS3Url { url: String },
    #[error("Could not determine bucket region for S3 bucket: s3://{bucket:?}, please specify with --aws-region")]
    CouldNotDetermineBucketRegion { bucket: String },
}
