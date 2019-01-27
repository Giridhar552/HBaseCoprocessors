# Replication

## Overview
This feature provides a prototype for Splice Machine's replication, which is based on HBase's native replication.


## Scope

In a brief summary, it contains following modules:

### Coprocessor ###
For Master Cluster's HBase servers, the Coprocessor needs to be installed. It can be put under the following path: `/opt/cloudera/parcels/CDH/lib/hadoop`

In order to install such Coprocessor on the running HBase. Following is the static way to do so:

* change the configuration of HBase by adding this name\value pair into the `property` and then `restart` the HBase service: 
`<property><name>hbase.coprocessor.regionserver.classes</name><value>org.splicemachine.capstone.coprocessor.GetRegionServerLSNEndpoint</value></property>`

### Replication Sink ###

