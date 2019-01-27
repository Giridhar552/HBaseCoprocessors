package org.splicemachine.capstone.clients;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.util.Bytes;
import org.splicemachine.capstone.GetRegionServerLSNProtos;
import org.splicemachine.capstone.benchmark.GetLSNTask;
import org.splicemachine.capstone.coprocessor.GetRegionServerLSNEndpoint;

import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

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
    static String outputFile = "/home/centos/EndpointOutput";

    public static void main(String[] args) throws  Exception{
        String slaveMasterAddr;
        // set threadNum to match the number of region servers
        int threadNum = 1;
        if(args.length != 1){
            System.out.println("DID NOT PROVIDE SLAVE HMASTER'S ADDRESS RUNNING WITHOUT UPDATING SLAVE'S masterLSNTS TABLE");
            startCollection(false, null, false, threadNum);
        }
        else {
            slaveMasterAddr = args[0];
            startCollection(true, slaveMasterAddr, false, threadNum);
        }
    }

    /*
     * update the masterLSNTS table at the slave cluster's HBase
     */
    static void updateSlaveTable(Connection conn, Map<String, Long> map, Long timestamp) throws Exception{
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
    static void getLSN(ThreadPoolExecutor executor, ConcurrentHashMap<String, Long> rsLSNMap,
                       Connection localConn, List<Connection> conns, int threadNum) throws Exception
    {
        rsLSNMap.clear();
        Admin myAdmin = localConn.getAdmin();
        ClusterStatus status = myAdmin.getClusterStatus();
        Collection<ServerName> rsNames = status.getServers();
        List<Callable<Integer>> todo = new ArrayList<Callable<Integer>>(rsNames.size());
        List<List<ServerName>> listOfServerNameLists = new ArrayList<List<ServerName>>();
        for(int i = 0; i < threadNum; ++i){
            listOfServerNameLists.add(new ArrayList<ServerName>());
        }
        int threadIdx = 0;
        for (ServerName rsName : rsNames) {
            listOfServerNameLists.get(threadIdx % threadNum).add(rsName);
        }
        for(int i = 0; i < threadNum; ++i){
            todo.add(new GetLSNTask(conns.get(i), rsLSNMap, listOfServerNameLists.get(i)));
        }
        List<Future<Integer>> res = executor.invokeAll(todo);
        myAdmin.close();
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



    public static void startCollection(boolean updateMasterLSNTS, String slaveMasterAddr, boolean takeLog, int threadNum) throws Exception{
        Connection slaveConn = null;
        Connection localConn = null;
        Configuration slaveConfig = HBaseConfiguration.create();
        Configuration localConfig = HBaseConfiguration.create();
        if(!updateMasterLSNTS){
            System.out.println("DID NOT PROVIDE SLAVE HMASTER'S ADDRESS RUNNING WITHOUT UPDATING SLAVE'S masterLSNTS TABLE");
        }
        else {
            // create connection to the Slave cluster's HBase
            slaveConfig.set("hbase.zookeeper.quorum", slaveMasterAddr);
            slaveConfig.set("hbase.master", slaveMasterAddr + ":60000");
            slaveConn = ConnectionFactory.createConnection(slaveConfig);
            createMasterLSNTSTable(slaveConn);
        }

        // create connection to the Master cluster's HBase
        localConn = ConnectionFactory.createConnection(localConfig);
        List<Connection> conns = new ArrayList<Connection>();
        for(int i = 0; i < threadNum; ++i){
            conns.add(ConnectionFactory.createConnection(localConfig));
        }
        ConcurrentHashMap<String, Long> result = new ConcurrentHashMap<String, Long>();
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadNum);

        // here we are using a pseudo timestamp
        int iterNum = 100;
        Long pseudoTs = 0l;
        long totalTime = 0;
        long updateTaleTotalTime = 0;
        int counter = 0;
        long startTime = 0;
        long endTime = 0;
        long updateTableStartTime = 0;
        try {
            while (true) {
                // Printout the result after running 10000 rounds.
                if(takeLog && counter % iterNum == 0 && counter > 0){
                    long avgRetrieveUpdateTime = totalTime / iterNum;
                    myPrint(String.format("retrieve %d regions and updateTable in %d nano sec for 100 rounds\n", result.size(), avgRetrieveUpdateTime),
                            takeLog);
//                    long avgUpdateTime = updateTaleTotalTime / 10000;
//                    myPrint(String.format("updateTable in %d nano sec for 10000 rounds\n", avgUpdateTime),
//                            takeLog);
                    totalTime = 0;
                    if(slaveMasterAddr == null){
                        exit(0);
                    }
                }

                startTime = System.nanoTime();
                getLSN(executor, result, localConn, conns, threadNum);
                if(updateMasterLSNTS) {
                    updateTableStartTime = System.nanoTime();
                    updateSlaveTable(slaveConn, result, pseudoTs);
                }
                endTime = System.nanoTime();
                if(updateMasterLSNTS){
                    updateTaleTotalTime += (endTime - updateTableStartTime);
                }

                totalTime += (endTime - startTime);
                counter++;
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
            return;
        }
    }

    static void myPrint(String str,boolean output){
        if(output){
            System.out.println(str);
        }
    }
}
