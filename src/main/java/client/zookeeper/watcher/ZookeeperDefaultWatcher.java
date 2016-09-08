package client.zookeeper.watcher;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 运行结果：
 * connected
 * node create /default-watcher
 * 111
 * 8589934672,8589934672,0
 * 111
 * 8589934672,8589934673,1
 * 
 * 结论：
 * zookeeper原生客户端支持默认watcher
 * 但无论是默认watcher，还是通过 getData()、getChildren()、exists()再注册的watcher，都是一次性的
 * 默认watcher会在会话创建后被触发，后续需要使用别的方式重新注册
 */
public class ZookeeperDefaultWatcher implements Watcher{
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private static ZooKeeper zk;
	private static Stat stat = new Stat();
	
	/**
	 * 处理Watcher监听方法
	 */
	@Override
	public void process(WatchedEvent event) {
		try{
			//连接状态
			if(event.getState() == KeeperState.SyncConnected){
				//会话创建
				if(event.getType()==EventType.None && event.getPath()==null){
					System.out.println("connected");
					
					//通过exists()重新注册默认watcher，否则无法监听到节点创建事件
					zk.exists("/default-watcher", true);
					
					connectedSemaphore.countDown();
				}
				//节点创建
				else if(event.getType()==EventType.NodeCreated){
					System.out.println("node create " + event.getPath());
				}
				//数据变更
				else if(event.getType()==EventType.NodeDataChanged){
					System.out.println(new String(zk.getData(event.getPath(), true, stat)));
					System.out.println(stat.getCzxid()+","+stat.getMzxid()+","+stat.getVersion());
				}
			}
		}
		catch(Exception e){
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception{
		zk = new ZooKeeper("172.18.100.176:8081,172.18.100.149:8081,172.18.100.169:8081", 
				            5000, new ZookeeperDefaultWatcher());
		
		//等待会话异步创建完成
		connectedSemaphore.await();
		
		String path = "/default-watcher";
		
		//创建节点
		zk.create(path, "111".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		//获取节点数据，并重新注册默认watcher
		System.out.println(new String(zk.getData(path, true, stat)));
		System.out.println(stat.getCzxid()+","+stat.getMzxid()+","+stat.getVersion());
		
		//更改节点数据
		zk.setData(path, "111".getBytes(), -1);
		
		Thread.sleep(5 * 1000);
	}

	
}
