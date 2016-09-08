package client.zookeeper.watcher;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

/**
 * 运行结果：
 * watcher2 321
 * watcher2 8589934676,8589934677,1
 * watcher1 321
 * watcher1 8589934676,8589934677,1
 *
 * 结论：
 * 对同一zookeeper客户端，统一path注册多个watcher，都可以生效
 */
public class ZookeeperMulitWatcherForOnePath {
	private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
	private static ZooKeeper zk;
	private static Stat stat = new Stat();
	
	/**
	 * Watcher实现类
	 */
	private static class ZkWatcher implements Watcher{
		private String name;
		private Stat stat = new Stat();
		
		public ZkWatcher(String name){
			this.name = name;
		}

		@Override
		public void process(WatchedEvent event) {
			try{
				//连接状态
				if(event.getState() == KeeperState.SyncConnected){
					//会话创建
					if(event.getType()==EventType.None && event.getPath()==null){
						System.out.println(name + " connected");
						connectedSemaphore.countDown();
					}
					//节点创建
					else if(event.getType()==EventType.NodeCreated){
						System.out.println(name + " node create " + event.getPath());
					}
					//数据变更
					else if(event.getType()==EventType.NodeDataChanged){
						System.out.println(name + " " + new String(zk.getData(event.getPath(), true, stat)));
						System.out.println(name + " " + stat.getCzxid()+","+stat.getMzxid()+","+stat.getVersion());
					}
				}
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception{
		ZkWatcher watcher1 = new ZkWatcher("watcher1");
		ZkWatcher watcher2 = new ZkWatcher("watcher2");
		
		zk = new ZooKeeper("172.18.100.176:8081,172.18.100.149:8081,172.18.100.169:8081", 
	            5000, null);
		
		String path = "/mulit-watcher";
		zk.create(path, "123".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
		
		//使用getData()对同一path注册两个watcher
		zk.getData(path, watcher1, stat);
		zk.getData(path, watcher2, stat);
		
		zk.setData(path, "321".getBytes(), -1);
		
		Thread.sleep(5 *1000);
	}
}
