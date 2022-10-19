package util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

public class CuratorUtil {
    private static CuratorFramework client;
    private static CuratorUtil instance;
    public static CuratorUtil getIntance(){
        if(instance==null){
            instance = new CuratorUtil();
            getClient();
        }
        return instance;
    }


    public static CuratorFramework getClient(){
        if(client == null){
            RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
            client = CuratorFrameworkFactory.builder().connectString("localhost:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .build();
            client.start();
        }
        return client;
    }





    public static String getAddrByTopicPartition(String topic,int partition) throws Exception {
        JSONObject jsonObject = JSONObject.parseObject(
            new String(client.getData().forPath("/brokers/topics/"+topic)));
        JSONArray partitions = jsonObject.getJSONObject("partitions").getJSONArray(String.valueOf(partition));
        // 已知leader 求地址
        JSONObject brokerInfo = JSONObject.parseObject(
            new String(client.getData().forPath("/brokers/ids/" + partitions.get(0))));
        return brokerInfo.getJSONArray("endpoints").get(0).toString();
    }
    public boolean tryCreateTemp(String path) throws Exception {
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
            return true;
        }catch (KeeperException.NodeExistsException e){
            // 出现异常说明创建失败
            System.out.println(Thread.currentThread()+"抢占失败");
            final NodeCache nodeCache = new NodeCache(client,path);
            //2.注册监听
            nodeCache.getListenable().addListener(() -> {
                System.out.println(Thread.currentThread()+"节点变化了继续抢占试试,~~~");
                nodeCache.close();
                tryCreateTemp(path);
            });
            //3.开启监听，如果设置为true，则开启监听加载缓存数据
            nodeCache.start(true);
            return false;
        }
    }

}
