package org.splicemachine.capstone.benchmark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.splicemachine.capstone.clients.EndpointClient;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.*;

public class CollectionBenchMark {
    final static String TestTablePrefix = "test_";
    final static byte[] TestTableCF = Bytes.toBytes("CF1");

    public static void main(String args[]) throws Exception{
        if(args.length != 1){
            throw new Exception("Missing argmument sepcifying the number of table to create");
        }
        int threadNum = 4;
        int tableNum = Integer.parseInt(args[0]);
        Configuration conf = HBaseConfiguration.create();
        int maxPartition = 20;
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(tableNum);
        List<List<Integer>> idLists = new ArrayList<List<Integer>>();
        for(int i = 0; i < maxPartition;++i){
            idLists.add(new ArrayList<Integer>());
        }
        for(int i = 0; i < tableNum; ++i){
            idLists.get(i%maxPartition).add(i);
        }
        for(int i = 0; i < maxPartition; ++i){
            TableCreationTask creationTask = new TableCreationTask(conf, idLists.get(i));
            executor.submit(creationTask);
        }
        executor.shutdown();
        while(!executor.isTerminated()){
            Thread.sleep(100);
        }

        EndpointClient.startCollection(false, null, true, threadNum);
    }
}
