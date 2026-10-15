# Silo Erasure Code Quickstart Guide

Silo protects data against hardware failures and silent data corruption using erasure coding and checksums. Recovery depends on the healthy shards and metadata remaining in each object's erasure set. With maximum parity, an object can tolerate the loss of up to `floor(N/2)` shards in its `N`-drive set; the deployment's total online drive count is not sufficient to determine recoverability.

## What is Erasure Code?

Erasure coding uses mathematical algorithms to reconstruct missing or corrupted data. Silo uses Reed-Solomon codes to split objects into data and parity shards. For a 12-drive erasure set, parity can range from `EC:0` (12 data shards and no parity) to `EC:6` (six data shards and six parity shards).

The default parity depends on the erasure set size: `EC:0` for 1 drive, `EC:1` for 2–3 drives, `EC:2` for 4–5 drives, `EC:3` for 6–7 drives, and `EC:4` for 8–16 drives. See the [Silo storage-class reference](https://silo.pgsty.com/reference/minio-server/settings/storage-class/) for configuration details.

In the 12-drive example, the default `EC:4` layout uses eight data shards and four parity shards. An existing object can tolerate four unavailable shards if the remaining shards and metadata are intact. Explicit `EC:6` instead uses six data and six parity shards, with a read quorum of six and a write quorum of seven.

## Why is Erasure Code useful?

Erasure coding protects objects against drive failures within their erasure set, up to the parity recorded for each object. Silo encodes objects individually and can heal them incrementally when enough healthy shards and consistent metadata remain. Failed drives still need repair or replacement to restore redundancy; erasure coding does not eliminate that maintenance.

![Erasure](https://github.com/pgsty/silo/blob/main/docs/screenshots/erasure-code.jpg?raw=true)

## What is Bit Rot protection?

Bit Rot, also known as data rot or silent data corruption is a data loss issue faced by disk drives today. Data on the drive may silently get corrupted without signaling an error has occurred, making bit rot more dangerous than a permanent hard drive failure.

Silo's erasure coded backend uses high speed [HighwayHash](https://github.com/minio/highwayhash) checksums to protect against Bit Rot.

## How are drives used for Erasure Code?

Silo divides the drives you provide into erasure-coding sets of *2 to 16* drives.  Therefore, the number of drives you present must be a multiple of one of these numbers.  Each object is written to a single erasure-coding set.

Silo uses the largest possible EC set size which divides into the number of drives given. For example, *18 drives* are configured as *2 sets of 9 drives*, and *24 drives* are configured as *2 sets of 12 drives*.  This is true for scenarios when running Silo as a standalone erasure coded deployment. In [distributed setup however node (affinity) based](https://silo.pgsty.com/operations/deployments/baremetal/) erasure stripe sizes are chosen.

The drives should all be of approximately the same size.

## Get Started with Silo in Erasure Code

### 1. Prerequisites

Install Silo - [Silo Quickstart Guide](https://silo.pgsty.com/operations/deployments/baremetal-deploy-minio-on-redhat-linux/)

### 2. Run Silo Server with Erasure Code

Example: Start Silo server in a 12 drives setup, using Silo binary.

```sh
silo server /data{1...12}
```

Example: Start Silo server in a 8 drives setup, using Silo Docker image.

```sh
podman run \
  -p 9000:9000 \
  -p 9001:9001 \
  --name silo \
  -v /mnt/data1:/data1 \
  -v /mnt/data2:/data2 \
  -v /mnt/data3:/data3 \
  -v /mnt/data4:/data4 \
  -v /mnt/data5:/data5 \
  -v /mnt/data6:/data6 \
  -v /mnt/data7:/data7 \
  -v /mnt/data8:/data8 \
  docker.io/pgsty/silo server /data{1...8} --console-address ":9001"
```

### 3. Test your setup

In a test deployment, take drives offline and verify reads and writes against the quorum required by each erasure set and object layout. Restore the drives after each test; continued I/O depends on the remaining healthy shards and metadata.
