# Distributed Silo Quickstart Guide [![Docker Pulls](https://img.shields.io/docker/pulls/pgsty/silo.svg?maxAge=604800)](https://hub.docker.com/r/pgsty/silo/)

Silo in distributed mode lets you pool multiple drives (even on different machines) into a single object storage server. As drives are distributed across several nodes, distributed Silo can withstand multiple node failures and yet ensure full data protection.

## Why distributed Silo?

Silo in distributed mode can help you setup a highly-available storage system with a single object storage deployment. With distributed Silo, you can optimally use storage devices, irrespective of their location in a network.

### Data protection

Distributed Silo provides protection against multiple node/drive failures and [bit rot](https://github.com/pgsty/silo/blob/main/docs/erasure/README.md#what-is-bit-rot-protection) using [erasure code](https://silo.pgsty.com/operations/concepts/erasure-coding/). As the minimum drives required for distributed Silo is 2 (same as minimum drives required for erasure coding), erasure code automatically kicks in as you launch distributed Silo.

With the default `availability` storage-class optimization, Silo can increase parity for new objects when drives are offline, up to half the drives in the erasure set. Writes must still meet the quorum for the resulting shard layout. This does not guarantee unchanged availability or keep a two-drive `EC:1` set writable after one drive fails. See the [storage-class reference](https://silo.pgsty.com/reference/minio-server/settings/storage-class/) for details.

### High availability

A stand-alone Silo server becomes unavailable if its host goes offline. In a distributed deployment, determine node-failure tolerance from how many drives each failed node removes from every erasure set and from the parity recorded for the affected objects. Aggregate online node or drive counts alone do not establish read or write quorum.

For example, if a 16-node pool has 16-drive erasure sets with one drive per node in each set, a fixed `EC:4` layout has a read and write quorum of 12. Losing four nodes leaves 12 drives in every set. This result depends on that drive placement and parity; it cannot be generalized to arbitrary failures of the same total number of drives. In a two-node, two-drive `EC:1` set, losing one node leaves only one drive, so existing intact objects can remain readable but writes cannot continue.

Refer to sizing guide for more understanding on default values chosen depending on your erasure stripe size [here](https://github.com/pgsty/silo/blob/main/docs/distributed/SIZING.md). Parity settings can be changed using [storage classes](https://github.com/pgsty/silo/tree/main/docs/erasure/storage-class).

### Consistency Guarantees

Silo follows strict **read-after-write** and **list-after-write** consistency model for all i/o operations both in distributed and standalone modes. This consistency model is only guaranteed if you use disk filesystems such as xfs, zfs or btrfs etc.. for distributed setup.

**In our tests we also found ext4 does not honor POSIX O_DIRECT/Fdatasync semantics, ext4 trades performance for consistency guarantees. Please avoid ext4 in your setup.**

**If Silo distributed setup is using NFS volumes underneath it is not guaranteed Silo will provide these consistency guarantees since NFS is not strictly consistent (If you must use NFS we recommend that you at least use NFSv4 instead of NFSv3 for relatively better outcomes).**

## Get started

If you're aware of stand-alone Silo set up, the process remains largely the same. Silo server automatically switches to stand-alone or distributed mode, depending on the command line parameters.

### 1. Prerequisites

Install Silo either on Kubernetes or Distributed Linux.

Install Silo on Kubernetes:

- [Silo Quickstart Guide for Kubernetes](https://silo.pgsty.com/operations/deployments/kubernetes/).
- [Deploy Silo on Kubernetes](https://silo.pgsty.com/operations/deployments/kubernetes/)

Install Distributed Silo on Linux:
- [Deploy Distributed Silo on Linux](https://silo.pgsty.com/operations/deployments/baremetal/#deploy-minio-distributed-baremetal)

### 2. Run distributed Silo

To start a distributed Silo instance, you just need to pass drive locations as parameters to the silo server command. Then, you’ll need to run the same command on all the participating nodes.

**NOTE:**

- All the nodes running distributed Silo should share a common root credentials, for the nodes to connect and trust each other. To achieve this, it is **recommended** to export root user and root password as environment variables, `MINIO_ROOT_USER` and `MINIO_ROOT_PASSWORD`, on all the nodes before executing Silo server command. If not exported, default `minioadmin/minioadmin` credentials shall be used.
- **Silo creates erasure-coding sets of _2_ to _16_ drives per set.  The number of drives you provide in total must be a multiple of one of those numbers.**
- **Silo chooses the largest EC set size which divides into the total number of drives or total number of nodes given - making sure to keep the uniform distribution i.e each node participates equal number of drives per set**.
- **Each object is written to a single EC set, and therefore is spread over no more than 16 drives.**
- **All the nodes running distributed Silo setup are recommended to be homogeneous, i.e. same operating system, same number of drives and same network interconnects.**
- Silo distributed mode requires **fresh directories**. If required, the drives can be shared with other applications. You can do this by using a sub-directory exclusive to Silo. For example, if you have mounted your volume under `/export`, pass `/export/data` as arguments to Silo server.
- The IP addresses and drive paths below are for demonstration purposes only, you need to replace these with the actual IP addresses and drive paths/folders.
- Servers running distributed Silo instances should be less than 15 minutes apart. You can enable [NTP](http://www.ntp.org/) service as a best practice to ensure same times across servers.
- `MINIO_DOMAIN` environment variable should be defined and exported for bucket DNS style support.
- Running Distributed Silo on **Windows** operating system is considered **experimental**. Please proceed with caution.

Example 1: Start a distributed Silo instance on n nodes with m drives each mounted at `/export1` to `/exportm`, by running this command on all participating nodes:

```mermaid
flowchart TB
    client["S3 clients"] --> endpoint["Load balancer or any Silo endpoint"]
    endpoint --> node1["Silo node 1"]
    endpoint --> node2["Silo node 2"]
    endpoint --> noden["Silo node n"]
    node1 --> drives1["export1 through exportm"]
    node2 --> drives2["export1 through exportm"]
    noden --> drivesn["export1 through exportm"]
```

### GNU/Linux and macOS

```sh
export MINIO_ROOT_USER=<ACCESS_KEY>
export MINIO_ROOT_PASSWORD=<SECRET_KEY>
silo server http://host{1...n}/export{1...m}
```

> **NOTE:** In above example `n` and `m` represent positive integers, _do not copy paste and expect it work make the changes according to local deployment and setup_.
> **NOTE:** `{1...n}` shown have 3 dots! Using only 2 dots `{1..n}` will be interpreted by your shell and won't be passed to Silo server, affecting the erasure coding order, which would impact performance and high availability. **Always use ellipses syntax `{1...n}` (3 dots!) for optimal erasure-code distribution**

### Expanding existing distributed setup

Silo supports expanding distributed erasure coded clusters by specifying new set of clusters on the command-line as shown below:

```sh
export MINIO_ROOT_USER=<ACCESS_KEY>
export MINIO_ROOT_PASSWORD=<SECRET_KEY>
silo server http://host{1...n}/export{1...m} http://host{o...z}/export{1...m}
```

For example:

```
silo server http://host{1...4}/export{1...16} http://host{5...12}/export{1...16}
```

Now the server has expanded total storage by _(newly_added_servers\*m)_ more drives, taking the total count to _(existing_servers\*m)+(newly_added_servers\*m)_ drives. New object upload requests automatically start using the least used cluster. This expansion strategy works endlessly, so you can perpetually expand your clusters as needed.  When you restart, it is immediate and non-disruptive to the applications. Each group of servers in the command-line is called a pool. There are 2 server pools in this example. New objects are placed in server pools in proportion to the amount of free space in each pool. Within each pool, the location of the erasure-set of drives is determined based on a deterministic hashing algorithm.

> **NOTE:** **Each pool you add must have the same erasure coding parity configuration as the original pool, so the same data redundancy SLA is maintained.**

## 3. Test your setup

To test this setup, access the Silo server via browser or [`mc`](https://silo.pgsty.com/reference/minio-mc/#quickstart).

## Explore Further

- [Silo Erasure Code QuickStart Guide](https://silo.pgsty.com/operations/concepts/erasure-coding/)
- [Use `mc` with Silo Server](https://silo.pgsty.com/reference/minio-mc/)
- [Use `aws-cli` with Silo Server](https://silo.pgsty.com/integrations/aws-cli-with-minio/)
- [Use `minio-go` SDK with Silo Server](https://silo.pgsty.com/developers/go/minio-go/)
- [The Silo documentation website](https://silo.pgsty.com/docs/)
