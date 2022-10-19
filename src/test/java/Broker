package server;

import com.alibaba.fastjson.JSONObject;

import util.CuratorUtil;

public class Broker {
    public void start() throws Exception {
        // broker启动去抢占controller节点
        CuratorUtil intance = CuratorUtil.getIntance();
        boolean testController = intance.tryCreateTemp("/testController");
        if(testController)
        System.out.println("我抢到controller节点了 我是主节点~~~");

        System.out.println("有节点挂了 看我进行选举");
        JSONObject jsonObject = JSONObject.parseObject(
            new String(CuratorUtil.getClient().getData().forPath("/brokers/topics/topic-dsc-collection/partitions/0/state")));
        Object isr = jsonObject.getJSONArray("isr").get(0);
        System.out.println("就是你了少年~ ");
        System.out.println("选举 isr里的第一个 "+ isr+" 为leader~ ");
    }
}
