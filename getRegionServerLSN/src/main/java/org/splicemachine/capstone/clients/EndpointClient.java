package org.splicemachine.capstone.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.splicemachine.capstone.GetRegionServerLSNProtos;
import org.splicemachine.capstone.coprocessor.GetRegionServerLSNEndpoint;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Timer;

import static java.lang.System.exit;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;

public class EndpointClient {
    // table at the slave cluster, stores each region's LSN and the timestamp
    // when the snapshot is taken.
    final static TableName masterLSNTS = TableName.valueOf("masterLSNTS");
    final static byte[] theCF = Bytes.toBytes("CF1");
    final static byte[] tsCol = Bytes.toBytes("ts");
    // only using one row for masterLSNTS table
    final static byte[] theRow = Bytes.toBytes("theRow");

    public static void main(String[] args) throws  IOException{
        // here we are using a pseudo timestamp
        Long pseudoTs = 0l;
        String slaveMasterAddr;
        if(args.length != 1){
            System.out.println("DID NOT PROVIDE SLAVE HMASTER'S ADDRESS");
            exit(1);
        }
        slaveMasterAddr = args[0];

        // create connection to the Master cluster's HBase
        Configuration localConfig = HBaseConfiguration.create();

        // create connection to the Slave cluster's HBase
        Configuration slaveConfig = HBaseConfiguration.create();
        slaveConfig.set("hbase.zookeeper.quorum", slaveMasterAddr);
        slaveConfig.set("hbase.master", slaveMasterAddr + ":60000");
        System.out.println("Slave and Master coinfigurations are created");

        Connection localConn = ConnectionFactory.createConnection(localConfig);
        Connection slaveConn = ConnectionFactory.createConnection(slaveConfig);
        System.out.println("Slave and Master connections are created");

        createMasterLSNTSTable(slaveConn);

        HashMap<String, Long> result = null;

        long total_time = 0;
        try {
            while (true) {
                long startTime = System.nanoTime();
                result = getLSN(localConn);
                updateSlaveTable(slaveConn, result, pseudoTs);
                long endTime = System.nanoTime();
                total_time += (endTime - startTime);
//                System.out.printf("retrieve %d regions in %d nano sec\n", result.size(), endTime - startTime);
                System.out.println(result.size());
                pseudoTs++;
            }
        }
        catch(Exception e){
            System.out.println(e.toString());
        }
        finally{
            // close connection upon failure
            if(localConn != null){
                localConn.close();
            }
            if(slaveConn != null){
                slaveConn.close();
            }
            System.out.printf("average time %d", total_time/100);
            return;
        }
    }


    /*
     * update the masterLSNTS table at the slave cluster's HBase
     */
    static void updateSlaveTable(Connection conn, HashMap<String, Long> map, Long timestamp) throws Exception{
        Table table = null;
        try{
            table = conn.getTable(masterLSNTS);
            // construct the single put
            Put rowUpdate = new Put(theRow);
            // add timestamp column into the put
            rowUpdate.addColumn(theCF, tsCol, Bytes.toBytes(timestamp));
            // add encoded region column into the put
            for(HashMap.Entry<String, Long> entry: map.entrySet()){
                rowUpdate.addColumn(theCF, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
            }
            table.put(rowUpdate);
        }
        finally{
            if(table != null){
                table.close();
            }
        }
    }


    /*
     * Make a map for each region server
     * For each region server, get a table, start key, end key that represents a specific region.
     * Then for each of these region, make a Coprocessor Call to the HBase and collect results.
     * If for one region server, no response has been received.
     */
    static HashMap<String, Long> getLSN(Connection connection) throws Exception {
        HashMap<String, Long> rsLSNMap = new HashMap<>();
            Admin myAdmin = connection.getAdmin();
//            System.out.println("Admin has been made");
            ClusterStatus status = myAdmin.getClusterStatus();
//            System.out.println("status has been got");
            Collection<ServerName> rsNames = status.getServers();
//            System.out.println("Got server names");
//            System.out.println(rsNames.toString());
            for (ServerName rsName : rsNames) {
                // Call to the RPC
                CoprocessorRpcChannel channel = myAdmin.coprocessorService(rsName);
                GetRegionServerLSNEndpoint.BlockingInterface service = GetRegionServerLSNEndpoint.newBlockingStub(channel);
                GetRegionServerLSNProtos.GetRegionServerLSNRequest request = GetRegionServerLSNProtos.GetRegionServerLSNRequest.getDefaultInstance();
                GetRegionServerLSNProtos.GetRegionServerLSNReponse response = service.getRegionServerLSN(null, request);
                List<GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result> resultList = response.getResultList();
                for (GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result result : resultList) {
                    rsLSNMap.put(result.getRegionName(), result.getLsn());
                }

            }

        return rsLSNMap;
    }

    /*
     * create the MasterLSNSTable at the Slave cluster if not already exists
     */
    public static void createMasterLSNTSTable(Connection connection) throws IOException{
        Admin admin = connection.getAdmin();
        if(!admin.tableExists(masterLSNTS)){
            HTableDescriptor tableDescriptor = new HTableDescriptor(masterLSNTS);
            HColumnDescriptor cf = new HColumnDescriptor(theCF);
            tableDescriptor.addFamily(cf);
            admin.createTable(tableDescriptor);
        }
        assert(admin.tableExists(masterLSNTS));
        admin.close();
    }

}
