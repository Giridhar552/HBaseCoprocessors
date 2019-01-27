package org.splicemachine.capstone.benchmark;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.splicemachine.capstone.GetRegionServerLSNProtos;
import org.splicemachine.capstone.coprocessor.GetRegionServerLSNEndpoint;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

public class GetLSNTask implements Callable<Integer> {
    private Connection conn;
    ConcurrentHashMap<String, Long> map;
    List<ServerName> rsNames;
    public GetLSNTask(Connection conn, ConcurrentHashMap<String, Long> map, List<ServerName> rsNames){
        this.conn = conn;
        this.map = map;
        this.rsNames = rsNames;
    }

    @Override
    public Integer call(){
        Admin admin = null;
        try{
            admin = this.conn.getAdmin();
            for(ServerName rsName : rsNames){
                CoprocessorRpcChannel channel = admin.coprocessorService(rsName);
                GetRegionServerLSNEndpoint.BlockingInterface service = GetRegionServerLSNEndpoint.newBlockingStub(channel);
                GetRegionServerLSNProtos.GetRegionServerLSNRequest request = GetRegionServerLSNProtos.GetRegionServerLSNRequest.getDefaultInstance();
                GetRegionServerLSNProtos.GetRegionServerLSNReponse response = service.getRegionServerLSN(null, request);
                List<GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result> resultList = response.getResultList();
                for (GetRegionServerLSNProtos.GetRegionServerLSNReponse.Result result : resultList) {
                    map.put(result.getRegionName(), result.getLsn());
                }
            }
        }
        catch(Exception e){
            e.printStackTrace();
        }
        finally{
            if(admin != null){
                try{
                    admin.close();
                }
                catch(IOException ioe){
                    ioe.printStackTrace();
                }
            }
            return 1;
        }


    }
}
