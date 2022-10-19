package consumer;

import com.alibaba.fastjson.JSONObject;

import util.CuratorUtil;

import org.apache.curator.framework.CuratorFramework;

public class Consumer {
    public void start() throws Exception {
        String groupid = "";
        int i = groupid.hashCode() % 50;
        System.out.println("当前组应该在第"+i+"个分区");
        // producer启动去zookeeper获取自己的partition的leader 比如topic-dsc-collection
        CuratorFramework client = CuratorUtil.getClient();
        JSONObject jsonObject = JSONObject.parseObject(
            new String(client.getData().forPath("/brokers/topics/topic-dsc-collection")));

        // 根据topic 跟partition 获得broker地址
        String broker = CuratorUtil.getAddrByTopicPartition("__consumer_offsets", i);
        // 往broker 发送joingroup 成为消费者leader
        System.out.println("往broker "+broker+" 发送joingroup 成为消费者leader");
    }
}
