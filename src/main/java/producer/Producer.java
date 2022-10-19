package producer;

import util.CuratorUtil;

public class Producer {
    public void start() throws Exception {
        // 先获得metadata 有几个broker 可以用来发数据

        // producer启动去zookeeper获取自己的partition的leader 比如topic-dsc-collection
        System.out.println("我要往" + CuratorUtil.getAddrByTopicPartition("topic-dsc-collection", 0) + "发送数据");
    }
}
