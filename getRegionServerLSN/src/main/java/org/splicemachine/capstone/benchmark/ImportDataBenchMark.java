package org.splicemachine.capstone.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.splicemachine.capstone.clients.EndpointClient;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ImportDataBenchMark {
    static final String DB_URL = "jdbc:splice://%S:1527/splicedb";
    static final String USER = "splice";
    static final String PASS = "admin";
    static final String INSTRUCTION = "dbAddr(one of the datanode in the Master cluster)" +
            "NumberOfTableToInsert slaveAddr";
    static final String LINEITEM_TABLE_NAME = "LINEITEM";
    static final String CREATE_LINEITEM_CMD = "CREATE TABLE %s " +
            "(L_ORDERKEY BIGINT NOT NULL," +
            " L_PARTKEY INTEGER NOT NULL," +
            " L_SUPPKEY INTEGER NOT NULL," +
            " L_LINENUMBER INTEGER NOT NULL," +
            " L_QUANTITY DECIMAL(15,2)," +
            " L_EXTENDEDPRICE DECIMAL(15,2)," +
            " L_DISCOUNT DECIMAL(15,2)," +
            " L_TAX DECIMAL(15,2)," +
            " L_RETURNFLAG VARCHAR(1)," +
            " L_LINESTATUS VARCHAR(1)," +
            " L_SHIPDATE DATE," +
            " L_COMMITDATE DATE," +
            " L_RECEIPTDATE DATE," +
            " L_SHIPINSTRUCT VARCHAR(25)," +
            " L_SHIPMODE VARCHAR(10)," +
            " L_COMMENT VARCHAR(44)" +
            ")";

    public static void main(String args[]) throws Exception{
        String dbAddr, slaveAddr;
        int tableNum;

        if(args.length != 3){
            throw new Exception("Missing argmument sepcifying the number of table to create");
        }
        try{
            dbAddr = args[0];
            tableNum = Integer.parseInt(args[1]);
            slaveAddr = args[2];
        }
        catch(Exception e){
            throw new Exception("Failed to parse input arguments");
        }
        // Check if the ClientDriver can be loaded successfully
        try{
            Class.forName("com.splicemachine.db.jdbc.ClientDriver");
        } catch(ClassNotFoundException cne){
            cne.printStackTrace();
            return; //exit early if we can't find the driver
        }

        // record the current tables in HBase
        Configuration conf = HBaseConfiguration.create();
        org.apache.hadoop.hbase.client.Connection hbaseConn =
                ConnectionFactory.createConnection(conf);
        HTableDescriptor[] descriptors = ReplicationBenchMark.getCurrTaleDescriptors(hbaseConn);
        HashSet<String> hTableNames = new HashSet<String>();
        for(HTableDescriptor des : descriptors){
            hTableNames.add(des.getNameAsString());
        }

        System.out.println("Continue to create ItemTable");
        System.in.read();
        // create LineItemTables through Splice
        createLineItemTable(dbAddr, tableNum);
        // create the same tables at slave cluster for replication.
        List<HTableDescriptor> newDescriptors = ReplicationBenchMark.getNewlyCreatedTableDescriptors(hbaseConn, hTableNames);
        ReplicationBenchMark.createTableAtSlaveCluster(newDescriptors, slaveAddr);
        ReplicationBenchMark.enableRep(hbaseConn,newDescriptors);
        hbaseConn.close();

        System.out.println("Continue to import data");
        System.in.read();
        // result queue for recording completion time
        Queue<Long> completionTimes = new ConcurrentLinkedQueue<Long>();
        // start multiple threads for insertion.
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(tableNum);
        for(int i = 0; i < tableNum; ++i){
             ImportDataTask task = new ImportDataTask(dbAddr, LINEITEM_TABLE_NAME+Integer.toString(i), completionTimes);
             executor.execute(task);
        }
        executor.shutdown();
        while(!executor.isTerminated()){
            Thread.sleep(100);
        }

        // out print the total and average time spent doing insertion
        long totalTime = 0l;
        for(long timeSpent: completionTimes){
            totalTime += timeSpent;
        }
        long avgTime = totalTime/tableNum;
        System.out.println(String.format("spent %d in total\n %d thread to %d table\n avg time %d",
                totalTime, tableNum, tableNum, avgTime));
    }


    /*
     * create tableNum of LineItem Tables
     */
    public static void createLineItemTable(String dbAddr, int tableNum) throws Exception
    {
        java.sql.Connection conn = null;
        String url = String.format(DB_URL, dbAddr);
        String testTableName;
        String createTableCmd;
        try{
            // create tables
            conn = DriverManager.getConnection(url, USER, PASS);
            DatabaseMetaData meta = conn.getMetaData();
            Statement stmt = conn.createStatement();
            for(int i = 0; i < tableNum; ++i){
                testTableName = LINEITEM_TABLE_NAME + Integer.toString(i);
                createTableCmd = String.format(CREATE_LINEITEM_CMD, testTableName);

                // check if table already exists or not
                ResultSet tables = meta.getTables(null,null, testTableName, null);
                if(tables.next()){
                    stmt.executeUpdate("drop table "+testTableName);
                }
                    stmt.executeUpdate(createTableCmd);
                    System.out.println("created " + testTableName);
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            if(conn != null){
                conn.close();
            }
        }
    }
}
