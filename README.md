# s3rename

s3rename is a tool to mass-rename keys within an S3 bucket.

The interface is designed to mimic the Perl [rename](https://www.unix.com/man-page/linux/1/prename/) utility on
GNU/Linux (also known as `prename` and `perl-rename`).

s3rename uses asynchronous requests to rename the keys in parallel, as
fast as possible.

The expression provided is applied to the entire key, allowing you to
rename parent "directories".

Object properties are preserved, unless the `--no-preserve-properties` 
flag is used.

Object ACL (Access Control List) settings will also be preserved, unless
the `--no-preserve-acl` flag is used.

It is highly recommended to use the `--dry-run` flag at first to ensure the
changes reflect what you intend.

## Usage

Note that regardless of the prefix used for filtering in the S3 URL
provided, the regex is applied to the __whole key__. This is necessary
to allow for full changes of the directory structure.

```
USAGE:
    s3rename [FLAGS] [OPTIONS] <expr> <s3-url>

FLAGS:
    -n, --dry-run                   Do not carry out modifications (only print)
    -h, --help                      Prints help information
        --no-anonymous-groups       Do not allow anonymous capture groups i.e. \1, \2 - may be useful when dealing with
                                    keys containing backslashes
        --no-preserve-acl           Do not preserve Object ACL settings (all will be set to private)
        --no-preserve-properties    Do not preserve object properties (saves retrieving per-object details) - using this
                                    flag will remove any encryption
    -q, --quiet                     Do not print key modifications
    -V, --version                   Prints version information
    -v, --verbose                   Print debug messages

OPTIONS:
        --aws-region <aws-region>    AWS Region (will be taken from bucket region if not overridden here)
        --canned-acl <canned-acl>    Canned access_control_list override - sets this ACL for all renamed keys [possible
                                     values: private, public-read, public-read-write, aws-exec-read, authenticated-read,
                                     bucket-owner-read, bucket-owner-full-control]

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

By default ACL settings for objects will be preserved (unless
`--no-preserve-acl` is passed), however this does
not apply to ACL settings which depend on the bucket ACL (i.e. public
write access).

The `--canned-acl <canned-acl>` option can be used to set the ACL of all
renamed objects to the provided [canned ACL](https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl).
Note that some canned ACLs are affected by bucket settings (such as
`public-read-write`).

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

$ ./s3rename 's/data_(?P<year>[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2}).txt/year=$year\/month=$month\/day=$day\/data_$year-$month-$day.txt/g' s3://s3rename-test-bucket/datatest
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

Note the use of single quotes for the sed regex string to avoid issues
with the $ symbols in the shell.

You can also use anonymous capture groups, with the replacement parts
marked either by $ or \, i.e.:

```
's/data_([0-9]{4})-([0-9]{2})-([0-9]{2}).txt/year=\1\/month=\2\/day=\3\/data_\1-\2-\3.txt/g'
```

is equivalent to the above, and equivalent to:


```
's/data_([0-9]{4})-([0-9]{2})-([0-9]{2}).txt/year=$1\/month=$2\/day=$3\/data_$1-$2-$3.txt/g'
```

Use multiple dollar symbols to escape the dollars (for literal dollar
symbols).

## Installation

s3rename depends on OpenSSL at runtime.

Building from source requires a Rust toolchain and Cargo.

If you use this tool please consider starring [the Github repo](https://github.com/jamesmcm/s3rename) and voting
for the package on [the AUR](https://aur.archlinux.org/packages/s3rename/).

### Arch Linux (via [the Arch User Repository (AUR)](https://aur.archlinux.org/packages/s3rename/))

Using the yay AUR helper:

```bash
$ yay -S s3rename
```

Alternatively you can [manually install the package from the AUR](https://wiki.archlinux.org/index.php/Arch_User_Repository#Installing_packages).

### Cargo (via [crates.io](https://crates.io/crates/s3rename))

```bash
$ cargo install s3rename
```

The `s3rename` binary will then be in your Cargo binaries directory (and
this should already be on your `$PATH`.

### Cargo (from this repository)

s3rename can be installed via Cargo from this cloned repository:

```bash
$ git clone git@github.com:jamesmcm/s3rename.git
$ cd s3rename
$ cargo install --path .
```

The `s3rename` binary will then be in your Cargo binaries directory (and
this should already be on your `$PATH`.

### Linux x86_64 binary

Static binaries compiled for Linux x86_64 are available in the [Github
releases](https://github.com/jamesmcm/s3rename/releases).

## Known Issues

* Buckets and objects using [S3 Object
  Lock](https://docs.aws.amazon.com/AmazonS3/latest/dev/object-lock-overview.html)
  are currently unsupported.
* Expiry rules set with prefixes in the bucket properties will not be 
  updated (so any keys moved out of the scope of these rules will no
  longer have the expiry rules applied). In the future a specific
  command to update expiry rules may be added.
* s3rename does not support custom encryption keys for encrypted buckets (i.e. if
  your encryption key is not generated and stored by AWS). This could be added in a
  future version.
* The rename operation is not fully atomic (since it involves
  separate CopyObject and DeleteObject requests) - this means that if
  s3rename is terminated suddenly during operation, the bucket could be left with
  copied files where the originals have not been renamed (re-running
  s3rename with the same arguments would fix this). 

## S3 Billing

s3rename operates on keys within the same bucket and so should trigger
no [data transfer costs](https://aws.amazon.com/s3/pricing/).

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
