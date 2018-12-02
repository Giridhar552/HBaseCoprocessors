package org.splicemachine.capstone.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.splicemachine.capstone.clients.EndpointClient;

import java.io.IOException;
import java.io.InputStreamReader;

public class CollectionBenchMark {
    final static String testTablePrefix = "test_";
    final static byte[] testTableCF = Bytes.toBytes("CF1");

    public static void main(String args[]) throws Exception{
        InputStreamReader cin = null;
        if(args.length != 1){
            throw new Exception("Missing argmument sepcifying the number of table to create");
        }
        int tableNum = Integer.parseInt(args[0]);
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        setupTable(tableNum, conn);
        conn.close();
        System.out.println("Table setup has finished");

        try{
            cin = new InputStreamReader(System.in);
            System.out.println("Enter q to quit. Enter other key to continue");
            char c;
            c = (char) cin.read();
            if(c == 'q'){
                System.exit(0);
            }
        }finally{
            if(cin != null){
                cin.close();
            }
        }

        EndpointClient.startCollection(false, null, true);

    }

    static void setupTable(int tableNum, Connection conn) throws IOException{
        Admin admin = conn.getAdmin();
        for(int i = 0; i < tableNum; i++){
            TableName tableName = TableName.valueOf(testTablePrefix + Integer.toString(i));
            if(!admin.tableExists(tableName)){
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                HColumnDescriptor cf = new HColumnDescriptor(testTableCF);
                tableDescriptor.addFamily(cf);
                admin.createTable(tableDescriptor);
            }
        }
        admin.close();
    }
}
