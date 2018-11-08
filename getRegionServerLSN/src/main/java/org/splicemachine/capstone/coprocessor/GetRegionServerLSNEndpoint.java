package org.splicemachine.capstone.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import org.apache.hadoop.hbase.wal.WAL;
import org.splicemachine.capstone.GetRegionServerLSNProtos;

import java.io.IOException;
import java.util.*;

public class GetRegionServerLSNEndpoint extends GetRegionServerLSNProtos.GetRegionServerLSNService
        implements Coprocessor, SingletonCoprocessorService
{
    private static final Log LOG = LogFactory.getLog(GetRegionServerLSNEndpoint.class);
    private RegionServerServices regionServerServices;
    private long testCount;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionServerCoprocessorEnvironment) {
            this.regionServerServices = ((RegionServerCoprocessorEnvironment) env).getRegionServerServices();
            this.testCount = 0;
            LOG.info("start is called and get executed");
        } else {
            throw new CoprocessorException("Must be loaded on a RegionServer!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        LOG.info("start is called and get executed");
        // nothing to do when coprocessor is shutting down
    }

    @Override
    public Service getService() {
        LOG.info("start is called and get executed");
        return this;
    }

    @Override
    public void getRegionServerLSN(RpcController controller,
                                   GetRegionServerLSNProtos.GetRegionServerLSNRequest request,
                                   RpcCallback<GetRegionServerLSNProtos.GetRegionServerLSNReponse> done) {
        // synchronized the call
        synchronized (this) {
            LOG.info("=====================getRegionServerLSN=====================");
            // Create builder
            GetRegionServerLSNProtos.GetRegionServerLSNReponse.Builder responseBuilder =
                    GetRegionServerLSNProtos.GetRegionServerLSNReponse.newBuilder();
            // Get Online Regions
            try {
                Set<Region> regionSet = new HashSet<>();
                // Get all the online tables in this RS
                Set<TableName> tableSet = this.regionServerServices.getOnlineTables();
                for (TableName tableName : tableSet) {
                    // get all the regions of this table on this RS
                    regionSet.addAll(this.regionServerServices.getOnlineRegions(tableName));
                }

                // Go through each Region on this RS
                for (Region region : regionSet) {
                    if (!region.isReadOnly()) {
                        // What should be the key value
                        WAL wal = regionServerServices.getWAL(region.getRegionInfo());
                        long earliestMemstoreNum = wal.getEarliestMemstoreSeqNum(region.getRegionInfo().getEncodedNameAsBytes());
                        String debugStr = String.format("checking %s with ReadPoint %d, maxFlusedSeqId %d, memStoreSeq %d",
                                region.getRegionInfo().getEncodedName(),region.getReadpoint(IsolationLevel.READ_COMMITTED),
                                region.getMaxFlushedSeqId(), earliestMemstoreNum);
                        LOG.info(debugStr);
                        long readPoint = region.getReadpoint(IsolationLevel.READ_COMMITTED);
                        String encodedRegionName = region.getRegionInfo().getEncodedName();
                        responseBuilder.addResult(
                                GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result.
                                        newBuilder().
                                        setLsn(readPoint).
                                        setRegionName(encodedRegionName).
                                        setValid(true).build()
                        );
                    }
                }
                GetRegionServerLSNProtos.GetRegionServerLSNReponse response = responseBuilder.build();
                done.run(response);
            }
            catch (IOException ioe) {
                LOG.error(ioe);
                // Call ServerRpcController#getFailedOn() to retrieve this IOException at client side.
                ResponseConverter.setControllerException(controller, ioe);
            }
        }
    }

    @Override
    public void testCall(RpcController controller, GetRegionServerLSNProtos.GetRegionServerLSNRequest request, RpcCallback<GetRegionServerLSNProtos.TestResponse> done) {
        synchronized (this){
            GetRegionServerLSNProtos.TestResponse.Builder responseBuilder =
                    GetRegionServerLSNProtos.TestResponse.newBuilder();
            this.testCount++;
            responseBuilder.setCount(this.testCount);
            GetRegionServerLSNProtos.TestResponse response = responseBuilder.build();
            done.run(response);
        }
    }
}
