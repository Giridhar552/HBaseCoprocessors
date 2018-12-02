package org.splicemachine.capstone.benchmark;

import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.splicemachine.capstone.clients.EndpointClient;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class ReplicationBenchMark {
    static final String JDBC_DRIVER = "com.splicemachine.db.jdbc.ClientDriver";
    static final String DB_URL = "jdbc:splice://%S:1527/splicedb";
    static final String SPLICE_NAMESPACE = "splice";
    static final String USER = "splice";
    static final String PASS = "admin";
    static final String TABLE_PREFIX = "TEST_";
    static final String INSTRUCTION = "dbAddr(one of the datanode in the Master cluster)" +
            "NumberOfTableToInsert slaveAddr insertNum";

    public static void main(String args[]) throws Exception
    {
        if(args.length != 4){
            System.out.println(String.format("Expected 4 arguments but get %d", args.length));
            System.out.println(INSTRUCTION);
            return;
        }
        String dbAddr, slaveAddr;
        int tableNum, insertNum;

        try{
            dbAddr = args[0];
            tableNum = Integer.parseInt(args[1]);
            slaveAddr = args[2];
            insertNum = Integer.parseInt(args[3]);
        }
        catch(Exception e){
            System.out.println(INSTRUCTION);
            throw new Exception("Failed to parse input arguments");
        }


        Configuration conf = HBaseConfiguration.create();
        org.apache.hadoop.hbase.client.Connection hbaseConn =
                ConnectionFactory.createConnection(conf);
        HTableDescriptor[] descriptors = getCurrTaleDescriptors(hbaseConn);
        HashSet<String> tableNames = new HashSet<String>();
        for(HTableDescriptor des : descriptors){
            tableNames.add(des.getNameAsString());
        }
        System.out.println("Continue to create table through spliceMachine");
        CollectionBenchMark.waitForInput();
        createTableTrhoughSplice(dbAddr, tableNum);
        List<HTableDescriptor> newDescriptors = getNewlyCreatedTableDescriptors(hbaseConn, tableNames);

        System.out.println("Continue to create the same table at the slave cluster");
        CollectionBenchMark.waitForInput();
        createTableAtSlaveCluster(newDescriptors, slaveAddr);
        enableRep(hbaseConn,newDescriptors);
        hbaseConn.close();

        // result queue for recording completion time
        Queue<Long> completionTimes = new ConcurrentLinkedQueue<Long>();
        // start multiple threads for insertion.

        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(tableNum);

        System.out.println("Continue to insert table");
        CollectionBenchMark.waitForInput();
        for(int i = 0; i < tableNum; ++i){
            InsertionTask task = new InsertionTask(dbAddr, TABLE_PREFIX + Integer.toString(i), completionTimes, insertNum);
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
        System.out.println(String.format("spent %d in total\n %d thread each inserts %d rows to %d table\n avg time %d",
                totalTime, tableNum, insertNum, tableNum, avgTime));
        return;
    }

    /*
     * return a list that contains all the HTableDescriptors
     */
    public static HTableDescriptor[] getCurrTaleDescriptors(org.apache.hadoop.hbase.client.Connection hbaseConn)
    throws IOException
    {
        Admin admin = hbaseConn.getAdmin();
        HTableDescriptor[] tableDescriptors = admin.listTableDescriptorsByNamespace("splice");
        admin.close();
        return tableDescriptors;
    }

    /*
     * use the given Splice db address to create the number of table indicated by tableNum if the table has not existed
     * yet.
     * The table has ten columns, each of them is an Integer
     */
    public static void createTableTrhoughSplice(String dbAddr, int tableNum) throws Exception
    {
        java.sql.Connection conn = null;
        String url = String.format(DB_URL, dbAddr);
        String tableName;
        try{
            // create tables
            conn = DriverManager.getConnection(url, USER, PASS);
            DatabaseMetaData meta = conn.getMetaData();
            Statement stmt = conn.createStatement();
            for(int i = 0; i < tableNum; ++i){
                tableName = TABLE_PREFIX + Integer.toString(i);
                // check if table already exists or not
                ResultSet tables = meta.getTables(null,null, tableName, null);
                if(!tables.next()){
                    stmt.executeUpdate("create table "  + tableName +
                            "(COL1 int, COL2 int, COL3 int, COL5 int, COL6 int, COL7 int, COL8 int, COL9 int, COL10 int)");
                }
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

    /*
     * find a list of HTableDescriptors that is not contained in the oldDescriptors
     */
    public static List<HTableDescriptor> getNewlyCreatedTableDescriptors(org.apache.hadoop.hbase.client.Connection hbaseConn,
                                                                     HashSet<String> oldDescriptors) throws IOException
    {
        List<HTableDescriptor> res = new ArrayList<HTableDescriptor>();
        HTableDescriptor[] currDescriptors = getCurrTaleDescriptors(hbaseConn);
        for(HTableDescriptor des : currDescriptors) {
            if(!oldDescriptors.contains(des.getNameAsString())){
                res.add(des);
            }
        }
        return res;
    }

    // For each table denoted in descriptors, enable replication
    public static void enableRep(org.apache.hadoop.hbase.client.Connection hbaseConn,
                                 List<HTableDescriptor> descriptors) throws Exception
    {
        Admin admin = null;
        ReplicationAdmin replicationAdmin = null;
        try{
            admin = hbaseConn.getAdmin();
            replicationAdmin = new ReplicationAdmin(hbaseConn.getConfiguration());
            for(HTableDescriptor descriptor: descriptors){
                TableName tableName = descriptor.getTableName();
                if(admin.tableExists(tableName)){
                    if(admin.isTableEnabled(tableName)){
                        admin.disableTable(tableName);
                    }
                    replicationAdmin.enableTableRep(tableName);
                    admin.enableTable(tableName);
                }
                else{
                    throw new Exception(String.format("Cannot find table %s which should have been created",
                            descriptor.getNameAsString()));
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            if(admin != null){
                admin.close();
            }
            if(replicationAdmin != null){
                replicationAdmin.close();
            }
        }

    }

    /*
     * create the same tables as the ones in the descriptors at the slave cluster
     */
    public static void createTableAtSlaveCluster(List<HTableDescriptor> descriptors, String slaveAddr)
            throws Exception
    {
        org.apache.hadoop.hbase.client.Connection slaveHbaseConn = null;
        Admin admin = null;
        try{
            Configuration slaveConf = HBaseConfiguration.create();
            slaveConf.set("hbase.zookeeper.quorum", slaveAddr);
            slaveConf.set("hbase.master", slaveAddr + ":60000");
            slaveHbaseConn = ConnectionFactory.createConnection(slaveConf);
            admin = slaveHbaseConn.getAdmin();
            // check if the name space exists or not
            String sampleName = descriptors.get(0).getNameAsString();
            int delim = sampleName.indexOf(':');
            if(delim < 0){
                throw new Exception("The table to be created at SlaveCluster's Hbase does not have a namespace!");
            }
            String namespace = sampleName.substring(delim + 1);
            if(!namespace.equals(SPLICE_NAMESPACE)){
                throw new Exception(String.format("Expected namespace %s but found %s", SPLICE_NAMESPACE, namespace));
            }

            // Check whether the namespace already created at the slave cluster or not. If not create a new one
            NamespaceDescriptor ns =
                    admin.getNamespaceDescriptor(SPLICE_NAMESPACE);
            if(ns == null){
                admin.createNamespace(NamespaceDescriptor.create(SPLICE_NAMESPACE).build());
            }

            for(HTableDescriptor descriptor : descriptors){
                if(!admin.tableExists(descriptor.getTableName())){
                    admin.createTable(descriptor);
                }
            }
        } catch(Exception e){
            e.printStackTrace();
        } finally{
            if(admin != null){
                admin.close();
            }
            if(slaveHbaseConn != null){
                slaveHbaseConn.close();
            }
        }
    }

}
