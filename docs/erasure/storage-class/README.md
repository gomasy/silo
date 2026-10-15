# Silo Storage Class Quickstart Guide

Silo server supports storage class in erasure coding mode. This allows configurable data and parity drives per object.

This page is intended as a summary of Silo Erasure Coding. For a more complete explanation, see <https://silo.pgsty.com/operations/concepts/erasure-coding/>.

## Overview

Silo supports two storage classes, Reduced Redundancy class and Standard class. These classes can be defined using environment variables
set before starting Silo server. After the data and parity drives for each storage class are defined using environment variables,
you can set the storage class of an object via request metadata field `x-amz-storage-class`. Silo server then honors the storage class by
saving the object in specific number of data and parity drives.

## Storage usage

The selection of varying data and parity drives has a direct impact on the drive space usage. With storage class, you can optimize for high
redundancy or better drive space utilization.

To get an idea of how various combinations of data and parity drives affect the storage usage, let’s take an example of a 100 MiB file stored
on 16 drive Silo deployment. If you use eight data and eight parity drives, the file space usage will be approximately twice, i.e. 100 MiB
file will take 200 MiB space. But, if you use ten data and six parity drives, same 100 MiB file takes around 160 MiB. If you use 14 data and
two parity drives, 100 MiB file takes only approximately 114 MiB.

Below is a list of data/parity drives and corresponding _approximate_ storage space usage on a 16 drive Silo deployment. The field _storage
usage ratio_ is simply the drive space used by the file after erasure-encoding, divided by actual file size.

| Total Drives (N) | Data Drives (D) | Parity Drives (P) | Storage Usage Ratio |
|------------------|-----------------|-------------------|---------------------|
|               16 |               8 |                 8 |                2.00 |
|               16 |               9 |                 7 |                1.79 |
|               16 |              10 |                 6 |                1.60 |
|               16 |              11 |                 5 |                1.45 |
|               16 |              12 |                 4 |                1.34 |
|               16 |              13 |                 3 |                1.23 |
|               16 |              14 |                 2 |                1.14 |

You can calculate _approximate_ storage usage ratio using the formula - total drives (N) / data drives (D).

### Allowed values for STANDARD storage class

`STANDARD` supports `EC:0` without erasure-code redundancy and nonzero parity values up to `floor(N/2)`, where `N` is the number of drives in the erasure set. When both `STANDARD` and `REDUCED_REDUNDANCY` parity are nonzero, `STANDARD` must be greater than or equal to `REDUCED_REDUNDANCY`; equal parity is allowed.

The default `STANDARD` parity is:

| Erasure Set Size | Default Parity (EC:M) |
| --- | --- |
| 1 | EC:0 |
| 2–3 | EC:1 |
| 4–5 | EC:2 |
| 6–7 | EC:3 |
| 8–16 | EC:4 |

For more complete documentation on Erasure Set sizing, see the [Silo Documentation on Erasure Sets](https://silo.pgsty.com/operations/concepts/erasure-coding/#minio-ec-erasure-set).

### Allowed values for REDUCED_REDUNDANCY storage class

`REDUCED_REDUNDANCY` parity can be zero or up to `floor(N/2)`. When both storage classes have nonzero parity, its parity must be less than or equal to `STANDARD` parity. The default is `EC:1` for multi-drive sets and `EC:0` for single-drive deployments. See the [Silo storage-class reference](https://silo.pgsty.com/reference/minio-server/settings/storage-class/) for the maintained configuration documentation.

## Get started with Storage Class

### Set storage class

The format to set storage class environment variables is as follows

`MINIO_STORAGE_CLASS_STANDARD=EC:parity`
`MINIO_STORAGE_CLASS_RRS=EC:parity`

For example, set `MINIO_STORAGE_CLASS_RRS` parity 2 and `MINIO_STORAGE_CLASS_STANDARD` parity 3

```sh
export MINIO_STORAGE_CLASS_STANDARD=EC:3
export MINIO_STORAGE_CLASS_RRS=EC:2
```

Storage class can also be set via `mc admin config` get/set commands to update the configuration. Refer [storage class](https://github.com/pgsty/silo/tree/main/docs/config#storage-class) for
more details.

#### Note

- If `STANDARD` storage class is set via environment variables or `mc admin config` get/set commands, and `x-amz-storage-class` is not present in request metadata, Silo server will
apply `STANDARD` storage class to the object. This means the data and parity drives will be used as set in `STANDARD` storage class.

- If storage class is not defined before starting Silo server, and subsequent PutObject metadata field has `x-amz-storage-class` present
with values `REDUCED_REDUNDANCY` or `STANDARD`, Silo server uses default parity values.

### Set metadata

In below example `minio-go` is used to set the storage class to `REDUCED_REDUNDANCY`. This means this object will be split across 6 data drives and 2 parity drives (as per the storage class set in previous step).

```go
s3Client, err := minio.New("localhost:9000", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true)
if err != nil {
 log.Fatalln(err)
}

object, err := os.Open("my-testfile")
if err != nil {
 log.Fatalln(err)
}
defer object.Close()
objectStat, err := object.Stat()
if err != nil {
 log.Fatalln(err)
}

n, err := s3Client.PutObject("my-bucketname", "my-objectname", object, objectStat.Size(), minio.PutObjectOptions{ContentType: "application/octet-stream", StorageClass: "REDUCED_REDUNDANCY"})
if err != nil {
 log.Fatalln(err)
}
log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")
```
