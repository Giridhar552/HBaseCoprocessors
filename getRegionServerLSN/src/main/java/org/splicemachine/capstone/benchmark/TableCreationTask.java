package org.splicemachine.capstone.benchmark;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.*;

class TableCreationTask implements Runnable{
    Configuration hbaseConf;
    List<Integer> tableIds;
    public TableCreationTask(Configuration hbaseConf, List<Integer> tableIds){
        this.hbaseConf = hbaseConf;
        this.tableIds = tableIds;
    }

    @Override
    public void run(){
        // No table_id is assigned to this thread
        if(this.tableIds.isEmpty()){
            return;
        }
        Connection conn = null;
        Admin admin = null;
        try{
            conn = ConnectionFactory.createConnection(this.hbaseConf);
            admin = conn.getAdmin();
            for(int tableId : this.tableIds){
                TableName tableName = TableName.valueOf(CollectionBenchMark.TestTablePrefix + Integer.toString(tableId));
                if(!admin.tableExists(tableName)){
                    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                    HColumnDescriptor cf = new HColumnDescriptor(CollectionBenchMark.TestTableCF);
                    tableDescriptor.addFamily(cf);
                    admin.createTable(tableDescriptor);
                }
            }
        }
        catch(IOException ioe){
            ioe.printStackTrace();
        }
        finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException ioe){
                    ioe.printStackTrace();
                }
            }
            if(conn != null){
                try{
                    conn.close();
                } catch(IOException ioe){
                    ioe.printStackTrace();
                }
            }
        }
    }
}