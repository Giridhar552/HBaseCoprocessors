package org.splicemachine.capstone.benchmark;

import org.apache.hadoop.hdfs.server.datanode.Replica;

import java.sql.CallableStatement;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Queue;

public class ImportDataTask implements Runnable{
    private String url;
    private String tableName;
    private Queue<Long> resQueue;
    private static String IMPORT_QUERY =
            "call SYSCS_UTIL.IMPORT_DATA "+
            "('splice', '%s', null, '/lineitem', ',', null, null, null, null, 0, '/badfiles', true, null)";

    public ImportDataTask(String dbAddr, String tableName, Queue<Long> resQueue) {
        this.url = String.format(ReplicationBenchMark.DB_URL, dbAddr);
        this.tableName = tableName;
        this.resQueue = resQueue;
    }

    @Override
    public void run()
    {
        java.sql.Connection conn = null;
        String importQuery = String.format(IMPORT_QUERY,  this.tableName);
        try {
            conn = DriverManager.getConnection(this.url,
                    ReplicationBenchMark.USER,
                    ReplicationBenchMark.PASS);
            CallableStatement cStmt = conn.prepareCall(importQuery);
            long startTime = System.nanoTime();
            cStmt.executeQuery();
            long endTime = System.nanoTime();
            this.resQueue.add((endTime - startTime));
            conn.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally{
            try{
                if(conn != null){
                    conn.close();
                }
            }
            catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
