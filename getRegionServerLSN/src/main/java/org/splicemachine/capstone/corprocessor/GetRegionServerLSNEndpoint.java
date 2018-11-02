package org.splicemachine.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;

import org.splicemachine.capstone.GetRegionServerLSNProtos;

import java.io.IOException;
import java.util.*;

public class GetRegionServerLSNEndpoint extends GetRegionServerLSNProtos.GetRegionServerLSNService
    implements Coprocessor, CoprocessorService
{
    private static final Log LOG = LogFactory.getLog(GetRegionServerLSNProtos.class);
    private RegionCoprocessorEnvironment env;
    private RegionServerServices regionServerServices;
    // TODO: use regionLSNMap to reduce the traffic
    private Map<String, Long>  regionLSNMap;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment)env;
            this.regionServerServices = ((RegionCoprocessorEnvironment) env).getRegionServerServices();
            this.regionLSNMap = new HashMap<String, Long>();
        } else {
            throw new CoprocessorException("Must be loaded on a RegionServer!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        // nothing to do when coprocessor is shutting down
    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void getRegionServerLSN(RpcController controller,
                            GetRegionServerLSNProtos.GetRegionServerLSNRequest request,
                            RpcCallback<GetRegionServerLSNProtos.GetRegionServerLSNReponse> done) {
        // Create builder
        GetRegionServerLSNProtos.GetRegionServerLSNReponse.Builder responseBuilder =
                GetRegionServerLSNProtos.GetRegionServerLSNReponse.newBuilder();
        // Get Online Regions
        try{
            Set<Region> regionSet = new HashSet<Region>();
            Set<TableName> tableSet = this.regionServerServices.getOnlineTables();
            for(TableName tableName : tableSet) {
                regionSet.addAll(this.regionServerServices.getOnlineRegions(tableName));
            }

            // Go through each Region on this Region Server
            for(Region region: regionSet){
                if(!region.isReadOnly()){
                    // What should be the key value
                    long maxCurrFlushedSeqId = region.getMaxFlushedSeqId();
                    String encodedRegionName = region.getRegionInfo().getEncodedName();
                    // add a new Result{maxCurrFlushedSeqId, encodedRegionName} to the reponse
                    responseBuilder.addResult(
                      GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result.
                              newBuilder().
                              setLsn(maxCurrFlushedSeqId).
                              setRegionName(encodedRegionName).build()
                    );
                }
            }
            // Build the result
            GetRegionServerLSNProtos.GetRegionServerLSNReponse response = responseBuilder.build();
            done.run(response);
        }
        catch (IOException ioe){
            LOG.error(ioe);
            // Call ServerRpcController#getFailedOn() to retrieve this IOException at client side.
            ResponseConverter.setControllerException(controller, ioe);
        }
    }
}
