import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Scanner;

public class CuratorTest {
    private CuratorFramework client;

    public CuratorFramework getClient(){
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
//        CuratorFramework client = CuratorFrameworkFactory.newClient("122.36.96.113:2181", 60 * 1000,
//                15 * 1000, retryPolicy);
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("localhost:2181")
                .sessionTimeoutMs(60 * 1000)
                .connectionTimeoutMs(15 * 1000)
                .retryPolicy(retryPolicy)
                .namespace("luzelong") //指定操作的根目录
                .build();
        return client;
    }

    /**
     * 建立连接
     * @Before:在所有Test方法调用前都会执行的方法
     */
    @Before
    public void testConnect(){
        client = getClient();
        client.start();
    }

    /**
     * 关闭连接
     * @After:在所有Test方法调用后都会执行的方法
     */
    @After
    public void close(){
        if (client != null) {
            client.close();
        }
    }
    @Test
    public void createTest() throws Exception {
        //由于client设置了namespace为luzelong，所以节点会创建在 /luzelong 下面

        //1.基本创建 ： 如果创建节点，没有指定数据，则默认将当前客户端的ip作为数据存储
        String path1 = client.create().forPath("/app1");
        //2.创建交接点带有数据
        String path2 = client.create().forPath("/app2","hhhh".getBytes());
        //3.设置节点的类型 ， 下面的案例是创建临时节点
        String path3 = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3","hhhh".getBytes());
        //4.创建多级节点  /app4/p1
        String path4 = client.create().creatingParentsIfNeeded().forPath("/app4/p1");

    }
    @Test
    public void testGet() throws Exception {
        //1.查询数据： get
        byte[] bytes = client.getData().forPath("/app1");
        System.out.println(new String(bytes));

        //2.查询子节点： ls
        List<String> strings = client.getChildren().forPath("/app4");
        System.out.println(strings);

        //3.查询节点状态: ls -s
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath("/app4/p1");
        System.out.println(stat);

    }
    @Test
    public void testSet() throws Exception {
        //1.修改数据
        client.setData().forPath("/app1","jjy".getBytes());

        //2.根据版本修改（需要先查询版本号，CAS思想）
        Stat stat = new Stat();
        client.getData().storingStatIn(stat).forPath("/app2");
        int version = stat.getVersion();
        client.setData().withVersion(version).forPath("/app2","yy".getBytes());
    }
    @Test
    public void testDel() throws Exception {
        //1.删除叶子节点  delete
        client.delete().forPath("/app1");

        //2.删除非叶子节点： deleteall
        client.delete().deletingChildrenIfNeeded().forPath("/app4");

        //3.必须成功删除
        client.delete().guaranteed().forPath("/app1");

        //4.带回调的删除
        client.delete().inBackground(new BackgroundCallback() {
            @Override
            public void processResult(CuratorFramework curatorFramework, CuratorEvent curatorEvent) throws Exception {
                System.out.println("我被删除了，curatorEvent="+curatorEvent);
            }
        }).forPath("/app1");
    }

    @Test
    public void testNodeCache() throws Exception {
        //1.创建NodeCache对象
        final NodeCache nodeCache = new NodeCache(client,"/app2");
        //2.注册监听
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("节点变化了~~~");
                byte[] data = nodeCache.getCurrentData().getData();
                System.out.println("修改节点后的数据为： " + new String(data));
            }
        });

        //3.开启监听，如果设置为true，则开启监听加载缓存数据
        nodeCache.start(true);

        new Scanner(System.in).next();
    }
    /**
     * 演示 pathChildrenCache：监听某个节点的所有子节点们（不会监听自己）
     */
    @Test
    public void testPathChildren() throws Exception {
        //1.创建监听对象
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client,"/",true);
        //2.绑定监听器
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent) throws Exception {
                System.out.println("子节点变化了~~~~");
                System.out.println(pathChildrenCacheEvent);

                //监听子节点的数据变更，并且拿到变更后的数据
                //1.获取类型
                PathChildrenCacheEvent.Type type = pathChildrenCacheEvent.getType();
                //2.判断类型是否是update
                if (type.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    byte[] data = pathChildrenCacheEvent.getData().getData();
                    System.out.println("修改后子节点的数据为：" + new String(data));
                }
            }
        });
        //3.开启监听
        pathChildrenCache.start();

        new Scanner(System.in).next();
    }
    /**
     * 演示TreeCache：监听某个节点自己和所有子节点们
     */
    @Test
    public void testTreeCache() throws Exception {
        //1.创建监听对象
        TreeCache treeCache = new TreeCache(client,"/app2");

        //2.注册监听
        treeCache.getListenable().addListener(new TreeCacheListener(){
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                System.out.println("节点变化了~~");
                System.out.println(treeCacheEvent);
            }
        });

        //3.开启
        treeCache.start();
        new Scanner(System.in).next();
    }

}
