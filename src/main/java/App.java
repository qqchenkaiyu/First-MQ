import consumer.Consumer;
import producer.Producer;
import server.Broker;
import util.CuratorUtil;

public class App {
    public static void main(String[] args) throws Exception {
        CuratorUtil intance = CuratorUtil.getIntance();
        new Broker().start();
        new Producer().start();
        new Consumer().start();
    }
}
