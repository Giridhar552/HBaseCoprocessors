package org.splicemachine.capstone.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.splicemachine.capstone.GetRegionServerLSNProtos;
import org.splicemachine.capstone.coprocessor.GetRegionServerLSNEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class EndpointClient {
    public static void main(String[] args) throws  IOException{
        HashMap<String, Long> result = getLSN();
        System.out.println(result.toString());
        return;
    }

    /*
     * Make a map for each region server
     * For each region server, get a table, start key, end key that represents a specific region.
     * Then for each of these region, make a Coprocessor Call to the HBase and collect results.
     * If for one region server, no response has been received.
     */
    static HashMap<String, Long> getLSN() throws IOException {
        HashMap<String, Long> rsLSNMap = new HashMap<>();
        Connection connection = null;
        try {
            Configuration configuration = HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum", "localhost");
            // Create a HBase connection
            connection = ConnectionFactory.createConnection(configuration);
            Admin myAdmin = connection.getAdmin();
            Collection<ServerName> rsNames = myAdmin.getClusterStatus().getServers();
            boolean res = SingletonCoprocessorService.class.isAssignableFrom(GetRegionServerLSNEndpoint.class);
            if(!res){
                //
            }
            for(ServerName rsName: rsNames){
                // Call to the RPC
                CoprocessorRpcChannel channel = myAdmin.coprocessorService(rsName);
                GetRegionServerLSNEndpoint.BlockingInterface service =  GetRegionServerLSNEndpoint.newBlockingStub(channel);
                GetRegionServerLSNProtos.GetRegionServerLSNRequest request = GetRegionServerLSNProtos.GetRegionServerLSNRequest.getDefaultInstance();
                GetRegionServerLSNProtos.GetRegionServerLSNReponse response = service.getRegionServerLSN(null, request);
                List<GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result> resultList = response.getResultList();
                for(GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result result : resultList){
                    rsLSNMap.put(result.getRegionName(), result.getLsn());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if(connection!=null){
                connection.close();
            }
            return rsLSNMap;
        }
    }

}
