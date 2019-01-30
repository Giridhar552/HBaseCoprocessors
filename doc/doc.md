# Replication

## Overview
This feature provides a prototype for Splice Machine's replication, which is based on HBase's native replication.



## Scope

In a brief summary, it contains following modules:

### Coprocessor ###
For master cluster's HBase servers, the Coprocessor needs to be installed. The jar file needs to be uploaded and can be put under the following path: `/opt/cloudera/parcels/CDH/lib/hadoop`

In order to install such Coprocessor on the running HBase. Following is the static way to do so:

change the configuration of HBase by adding this name\value pair into the `property` and then `restart` the HBase service: 
`<property><name>hbase.coprocessor.regionserver.classes</name><value>org.splicemachine.capstone.coprocessor.GetRegionServerLSNEndpoint</value></property>`

### Replication Sink ###
For the slave cluster, a znode for the customed replication sinks will be created and acting as if it were a znode for a HBase cluster.

For slave cluster's HBase servers, the `replicator-jar-with-dependencies.jar` jar file needs to be uploaded.

Run the `main` method in `RepWALEditConsumer` class. This class will expose the server as a HBase Server. Therefore, it can be found by the master cluster's HBase and accept the `WAL`s from it.

### Snapshot ###
For taking snapshot, start the `main` method in `EndpointClient` class at master cluster's server. The process will first collect a `pseudo timestamp` (which is an long that increments itself everytime it is collected, in practice, the timestamp should be acquired from the `Splice Machine` that is running at the master cluster) the master cluster. The process will then collect every `Region`'s `LSN` and store them in the memory. Once the collection is done, the process then store the collected `Region`s' encoded names, the corresponding `LSN`s in `masterLSNTS` table, and the collected `timestamp` at the slave cluster.

### Increase Timestamp ###
Run the `main` method in `TimestampIncre` class. `masterLSNTS` table is used to keep track of `snapshot` inforamtion from the master cluster. The process will keep reading `masterLSNTS` and `regionLSNTable` tables, comparing the `LSN` `Region` by `Region`. The process bumps up the `timestamp` at the slave cluster if and only if for every `Region` in `regionLSNTable`, the `LSN` is larger than or equal to value in `masterLSNTS` table.

### Replicate a Table ###
The replication mechanism is actually using the HBase's native replication functionality. When replicating a `Splice Table A`, the replication mechanism is actually replicating table `A`'s underlying HBase table `a`.

In order to replicate a HBase table `t1` from a master cluster to a slave cluster, four steps are needed.[Here is the Cloudera's documentation about HBase replication, it contains detailed description for each of the steps](https://www.cloudera.com/documentation/enterprise/5-14-x/topics/cdh_bdr_hbase_replication.html):
1. Both the master and the slave HBase clusters' `hbase.replication` values need to be set to `True`.
2. For every `column` in `t1` at the master cluster, the `replication scope` needs to be set to `1`.
3. At the slave cluster, a table named should be created with the same name and schema as the master cluster's table `t1`.
4. Add the slave cluster as a peer for replication.

An example that contains step 2,3,4 can be found in the `ReplicationBenchMark` class.