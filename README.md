# s3rename

s3rename is a tool to mass-rename keys within an S3 bucket.

The interface is designed to mimic the Perl [rename](https://www.unix.com/man-page/linux/1/prename/) utility on
GNU/Linux (also known as `prename` and `perl-rename`).

s3rename uses asynchronous requests to rename the keys in parallel, as
fast as possible.

## Usage

```
USAGE:
    s3rename [FLAGS] [OPTIONS] <expr> <s3-url>

FLAGS:
    -n, --dry-run    Do not carry out modifications (only print)
    -h, --help       Prints help information
    -q, --quiet      Do not print key modifications
    -V, --version    Prints version information
    -v, --verbose    Print debug messages

OPTIONS:
        --aws-region <aws-region>    AWS Region (will be taken from bucket region if not overridden here)

ARGS:
    <expr>      Perl RegEx Replace Expression (only s/target/replacement/flags form supported)
    <s3-url>    S3 URL: s3://bucket-name/optional-key-prefix
```

### Examples

s3rename uses the Perl regular expression format (like sed) to rename
files:

```
$ aws s3 ls s3://s3rename-test-bucket --recursive
2020-05-01 12:30:25         16 testnewfile.txt

$ ./s3rename "s/new/old" s3://s3rename-test-bucket/test
Renaming testnewfile.txt to testoldfile.txt

$ aws s3 ls s3://s3rename-test-bucket --recursive
2020-05-01 12:33:48         16 testoldfile.txt
```

The `--dry-run` flag will print changes to be made without carrying them
out. This is __highly__ recommended before running changes.

### Renaming flat files to a nested directory structure for AWS Glue

This program was originally inspired by the need to rename the keys of 
thousands of files which were stored in a flat structure, so that they
could be correctly parsed by AWS Glue which requires a nested structure 
with the "directory" names corresponding to the partitions.

```
$ aws s3 ls s3://s3rename-test-bucket/datatest --recursive
2020-05-01 12:38:33          0 datatest/
2020-05-01 12:38:43          0 datatest/data_2020-04-01.txt
2020-05-01 12:38:43          0 datatest/data_2020-04-02.txt
2020-05-01 12:38:43          0 datatest/data_2020-04-03.txt
2020-05-01 12:38:43          0 datatest/data_2020-04-04.txt
2020-05-01 12:38:43          0 datatest/data_2020-04-05.txt
2020-05-01 12:38:43          0 datatest/data_2020-05-01.txt
2020-05-01 12:38:43          0 datatest/data_2020-05-02.txt
2020-05-01 12:38:43          0 datatest/data_2020-06-01.txt

$ ./s3rename "s/data_(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2}).txt/year=\$year\/month=\$month\/day=\$day\/data_\$year-\$month-\$day.txt/g" s3://s3rename-test-bucket/datatest
Renaming datatest/ to datatest/
Renaming datatest/data_2020-04-01.txt to datatest/year=2020/month=04/day=01/data_2020-04-01.txt
Renaming datatest/data_2020-04-02.txt to datatest/year=2020/month=04/day=02/data_2020-04-02.txt
Renaming datatest/data_2020-04-03.txt to datatest/year=2020/month=04/day=03/data_2020-04-03.txt
Renaming datatest/data_2020-04-04.txt to datatest/year=2020/month=04/day=04/data_2020-04-04.txt
Renaming datatest/data_2020-04-05.txt to datatest/year=2020/month=04/day=05/data_2020-04-05.txt
Renaming datatest/data_2020-05-01.txt to datatest/year=2020/month=05/day=01/data_2020-05-01.txt
Renaming datatest/data_2020-05-02.txt to datatest/year=2020/month=05/day=02/data_2020-05-02.txt
Renaming datatest/data_2020-06-01.txt to datatest/year=2020/month=06/day=01/data_2020-06-01.txt

$ aws s3 ls s3://s3rename-test-bucket/datatest --recursive
2020-05-01 12:38:33          0 datatest/
2020-05-01 12:39:38          0 datatest/year=2020/month=04/day=01/data_2020-04-01.txt
2020-05-01 12:39:38          0 datatest/year=2020/month=04/day=02/data_2020-04-02.txt
2020-05-01 12:39:38          0 datatest/year=2020/month=04/day=03/data_2020-04-03.txt
2020-05-01 12:39:38          0 datatest/year=2020/month=04/day=04/data_2020-04-04.txt
2020-05-01 12:39:38          0 datatest/year=2020/month=04/day=05/data_2020-04-05.txt
2020-05-01 12:39:38          0 datatest/year=2020/month=05/day=01/data_2020-05-01.txt
2020-05-01 12:39:38          0 datatest/year=2020/month=05/day=02/data_2020-05-02.txt
2020-05-01 12:39:38          0 datatest/year=2020/month=06/day=01/data_2020-06-01.txt
```

Note the requirement to use named capture groups, this will be addressed
in a future version to allow numbered anonymous capture groups like sed.

## Known Issues

* Currently the rename operation is not atomic (since it involves
  separate CopyObject and DeleteObject requests) - this means that if
  s3rename is terminated during operation, the bucket could be left with
  copied files where the originals have not been renamed (re-running
  s3rename with the same arguments would fix this). This will be
  addressed in a future version.
* s3rename does not work with encrypted buckets (either by KMS or S3
  encryption) - this support will be added in a future version.
* Object ACLs (Access Control Lists) are currently overwritten, so all
  objects renamed will be set to Private. This will be addressed in a
  future version, but will likely require more requests and so be put
  behind a --keep-acl flag.
* The --no-overwrite flag is currently unimplemented, so there is no way
  to rename keys, but not carry out the rename if a key already exists
  with that name. This will be addressed in a future version.
* There is no way of filtering keys to be renamed based on their date
  modified, etc. - this will be addressed in a future version.

s3rename uses CopyObject and DeleteObject(s) requests directly, this
means that 

## S3 Billing

s3rename operates on keys within the same bucket and so should trigger
no data transfer costs.

Whilst it does use CopyObjectRequests to carry out the renaming, the
additional data does not exist for long and should trigger no costs for
data usage:

Regarding billing for data storage, the [S3 Billing documentation](https://aws.amazon.com/s3/faqs/#Billing) states:

> The volume of storage billed in a month is based on the average storage used throughout the month.
> This includes all object data and metadata stored in buckets that you created under your AWS account.
> We measure your storage usage in “TimedStorage-ByteHrs,” which are added up at the end of the month to generate your monthly charges.

## License

s3rename is licensed under either of:

* Apache License, Version 2.0 (LICENSE-APACHE or http://www.apache.org/licenses/LICENSE-2.0)
* MIT license (LICENSE-MIT or http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license,
shall be dual licensed as above, without any additional terms or conditions.
