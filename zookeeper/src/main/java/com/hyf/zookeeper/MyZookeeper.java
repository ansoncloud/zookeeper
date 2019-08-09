package com.hyf.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 把zookeeper-3.4.6 目录下的zookeeper-3.4.6.jar包 和 zookeeper-3.4.6\lib 目录下的jar包全复制到工程里
 * 封装一个zookeeper的操作类，类中支持 a) 创建关闭连接 b) 读取指定节点数据 c) 读取指定节点的子节点 d) 判断节点是否存在 e) 创建节点、更新节点数据、删除节点 f)
 * 所有的监听都交给连接创建的全局观察者即可 写一个主函数进行测试
 * @author 黄永丰
 * @createtime 2015年12月21日
 * @version 1.0
 */
public class MyZookeeper implements Watcher
{
	private static final int SESSION_TIMEOUT = 10000;
//	private static final String CONNECTION_STRING = "zookeeper01:2181,zookeeper02:2181,zookeeper03:2181";
	private static final String CONNECTION_STRING = "192.168.33.42:2181";//现我的是伪分布式
	private static final String ZK_PATH = "/test";
	private ZooKeeper zk = null;
	private CountDownLatch connectedSemaphore = new CountDownLatch(1);

	/**
	 * 创建ZK连接
	 * @param connectString ZK服务器地址列表
	 * @param sessionTimeout Session超时时间
	 */
	public void createConnection(String connectString, int sessionTimeout)
	{
		this.releaseConnection();
		try
		{
			zk = new ZooKeeper(connectString, sessionTimeout, this);
			connectedSemaphore.await();
		}
		catch (InterruptedException e)
		{
			System.out.println("连接创建失败，发生 InterruptedException");
			e.printStackTrace();
		}
		catch (IOException e)
		{
			System.out.println("连接创建失败，发生 IOException");
			e.printStackTrace();
		}
	}

	/**
	 * 关闭ZK连接
	 */
	public void releaseConnection()
	{
		if (this.zk != null)
		{
			try
			{
				this.zk.close();
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	/**
	 * 创建节点
	 * @param path 节点path
	 * @param data 初始数据内容
	 * @return
	 */
	public boolean createPath(String path, String data)
	{
		try
		{
			System.out.println("节点创建成功, Path: " 
						+ this.zk.create(path,data.getBytes(),Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL) 
						+ ", content: " + data);
		}
		catch (KeeperException e)
		{
			System.out.println("节点创建失败，发生KeeperException");
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			System.out.println("节点创建失败，发生 InterruptedException");
			e.printStackTrace();
		}
		return true;
	}

	/**
	 * 读取指定节点数据内容
	 * @param path 节点path
	 * @return
	 */
	public String readData(String path)
	{
		try
		{
			System.out.println("获取数据成功，path：" + path);
			return new String(this.zk.getData(path, false, null)); // 注意这个null，这里可以设置watcher
		}
		catch (KeeperException e)
		{
			System.out.println("读取数据失败，发生KeeperException，path: " + path);
			e.printStackTrace();
			return "";
		}
		catch (InterruptedException e)
		{
			System.out.println("读取数据失败，发生 InterruptedException，path: " + path);
			e.printStackTrace();
			return "";
		}
	}

	/**
	 * 读取指定节点子节点
	 * @param path 节点path
	 * @return
	 */
	public List getChildList(String path)
	{
		try
		{
			System.out.println("获取成功，path：" + path);
			return zk.getChildren(path, true);// 这里可以设置watcher, zk.getChildren(path, new MyWatcher()) ;
		}
		catch (Exception e)
		{
			System.out.println("读取失败，发生Exception，path: " + path);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 判断节点是否存在
	 * @param path 节点path
	 * @return
	 */
	public Stat isExist(String path)
	{
		try
		{
			System.out.println("成功，path：" + path);
			return zk.exists(path, true);// 这里可以设置watcher,zk.exists(path, new MyWatcher());
		}
		catch (Exception e)
		{
			System.out.println("失败，发生Exception，path: " + path);
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 更新指定节点数据内容
	 * @param path 节点path
	 * @param data 数据内容
	 * @return
	 */
	public boolean writeData(String path, String data)
	{
		try
		{
			System.out.println("更新数据成功，path：" + path + ", stat: " + this.zk.setData(path, data.getBytes(), -1));
		}
		catch (KeeperException e)
		{
			System.out.println("更新数据失败，发生KeeperException，path: " + path);
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			System.out.println("更新数据失败，发生 InterruptedException，path: " + path);
			e.printStackTrace();
		}
		return false;
	}

	/**
	 * 删除指定节点
	 * @param path 节点path
	 */
	public void deleteNode(String path)
	{
		try
		{
			this.zk.delete(path, -1);
			System.out.println("删除节点成功，path：" + path);
		}
		catch (KeeperException e)
		{
			System.out.println("删除节点失败，发生KeeperException，path: " + path);
			e.printStackTrace();
		}
		catch (InterruptedException e)
		{
			System.out.println("删除节点失败，发生 InterruptedException，path: " + path);
			e.printStackTrace();
		}
	}

	/**
	 * 收到来自Server的Watcher通知后的处理。
	 */
	public void process(WatchedEvent event)
	{
		System.out.println("收到事件通知：" + event.getState() + "\n");
		if (KeeperState.SyncConnected == event.getState())
		{
			connectedSemaphore.countDown();
		}
	}

	//测试
	public static void main(String[] args)
	{
		MyZookeeper sample = new MyZookeeper();
		sample.createConnection(CONNECTION_STRING, SESSION_TIMEOUT);
		if (sample.createPath(ZK_PATH, "我是节点初始内容"))
		{
			System.out.println();
			System.out.println("数据内容: " + sample.readData(ZK_PATH) + "\n");
			sample.writeData(ZK_PATH, "更新后的数据");
			System.out.println("数据内容: " + sample.readData(ZK_PATH) + "\n");
			sample.deleteNode(ZK_PATH);
		}
		sample.releaseConnection();
	}

}
