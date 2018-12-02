package org.splicemachine.capstone.benchmark;
import java.sql.*;
import java.util.*;
public class InsertionTask implements Runnable{
    private String tableName;
    private String url;
    private int insertNum;
    private Queue<Long> resQueue;

    public InsertionTask(String dbAddr,String tableName, Queue<Long> resQueue, int insertNum)
    {
        this.tableName = tableName;
        this.url = String.format(ReplicationBenchMark.DB_URL,dbAddr);
        this.resQueue = resQueue;
        this.insertNum = insertNum;
    }

    @Override
    public void run()
    {
        java.sql.Connection conn = null;
        try {
            conn = DriverManager.getConnection(this.url,
                    ReplicationBenchMark.USER,
                    ReplicationBenchMark.PASS);
            conn.setAutoCommit(true);
            PreparedStatement pstmt = conn.prepareStatement("insert into" + tableName +
                    "values(?,?,?,?,?,?,?,?,?,?)");
            long startTime = System.nanoTime();
            for(int colVal = 0; colVal < this.insertNum; ++colVal){
                for(int i = 1; i <= 10; ++i){
                    pstmt.setInt(i, colVal);
                }
                pstmt.executeUpdate();
            }
            long endTime = System.nanoTime();
            this.resQueue.add(endTime);
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
