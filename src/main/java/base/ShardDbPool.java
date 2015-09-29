package com.chanjet.imp.dao.mongo.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanjet.imp.configcenter.ZkHelp;
import com.chanjet.imp.utils.DefaultStringUtils;
import com.github.zkclient.IZkDataListener;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

public class ShardDbPool {

	private final static Logger logger = LoggerFactory.getLogger(DbPool.class);
	private static ShardDbPool instance = null;
	// public List<MongoClient> listMongoClient = null;
	public MongoClient mongoClient = null;
	private int poolSize = 40;
	private int blockSize = 5;
	// private boolean autoConnectRetry = true;
	private int connectTimeout = 3000;
	private int socketTimeout = 5000;
	private int maxWaitTime = 5000;
	public String dbName = "";
	public String userName = "";
	public String pwd = "";
	public boolean authed = true;
	private final static ZkHelp zkHelp = ZkHelp.getInstance();
	// public final static String mongosIp = "127.0.0.1:50000:mydb:hujh:hujh";
	
	private ShardDbPool() {
	}
	
	public synchronized static ShardDbPool getInstance(String zkPath){
		if (instance == null) {
			instance = new ShardDbPool();
			IZkDataListener listener = new IZkDataListener() {
				public void handleDataDeleted(String dataPath) throws Exception {
					logger.info("!!! mongo node data has been deleted !!!"
							+ dataPath);
				}

				public void handleDataChange(String dataPath, byte[] data)
						throws Exception {
					logger.info("!!! mongo node data has been changed !!!"
							+ dataPath);
					String redisServerInf = DefaultStringUtils.toStr(data);
					instance.initialPool(dataPath);
					logger.info("!!! mongo node[" + redisServerInf
							+ "] connection pool has been rebuild !!!"
							+ dataPath);
				}
			};
			// 节点添加监控
			zkHelp.subscribeDataChanges(zkPath, listener);
			// 构建dbpool
			instance.initialPool(zkPath);
		}
		return instance;
	}
	
	/**
	public synchronized static ShardDbPool getInstance(String zkPath) {
		if (instance == null) {
			instance = new ShardDbPool();
			IZkDataListener listener = new IZkDataListener() {
				public void handleDataDeleted(String dataPath) throws Exception {
					logger.info("!!! mongo node data has been deleted !!!"
							+ dataPath);
				}

				public void handleDataChange(String dataPath, byte[] data)
						throws Exception {
					logger.info("!!! mongo node data has been changed !!!"
							+ dataPath);
					String redisServerInf = StringUtils.toStr(data);
					instance.initialPool(dataPath);
					logger.info("!!! mongo node[" + redisServerInf
							+ "] connection pool has been rebuild !!!"
							+ dataPath);
				}
			};
			// 节点添加监控
			zkHelp.subscribeDataChanges(zkPath, listener);
			// 初始化 公用redis集群
			instance.initialPool(zkPath);
		}
		return instance;
	}
	**/

	/**
	 * 初始化连接池，设置参数。 读zookeeper
	 */
	public void initialPool(String zkPath) {
		logger.info("start init connection pools");
		// 如果不为null 先清理再建立
		if (mongoClient != null) {
			mongoClient.close();
		}
		List<ServerAddress> listServerAddresses = null;
		try {
			// mongodb连接串中间用空,开 ,多个实例用|分割
			listServerAddresses = constructConnectionAddress(zkPath);
			
			 MongoCredential cr = MongoCredential.createCredential(userName, dbName, pwd.toCharArray());
		     List<MongoCredential> list_mc = new ArrayList<MongoCredential>();
		     list_mc.add(cr);
			
			// MongoClient mongoClient = null;
			// 其他参数根据实际情况进行添加
			MongoClientOptions mco = new MongoClientOptions.Builder()
					// .autoConnectRetry(autoConnectRetry)
					.writeConcern(WriteConcern.SAFE)
					.connectionsPerHost(poolSize)
					.threadsAllowedToBlockForConnectionMultiplier(blockSize)
					.connectTimeout(connectTimeout)
					.socketTimeout(socketTimeout).maxWaitTime(maxWaitTime)
					.build();
			// mongoClient = new MongoClient(listServerAddresses, mco);
			// authDb(mongoClient, userName, pwd);
			mongoClient = new MongoClient(listServerAddresses, list_mc, mco);
			logger.info("end init connection pools");
		} catch (Exception e) {
			logger.error("构造连接池出错 listServerAddresses:{}", listServerAddresses);
			logger.error("构造连接池出错" , e);
			e.printStackTrace();
		}
	}

	/**
	 * 获取db容器
	 * 
	 * @param key
	 * @return
	 */
	
//	public void authDb(MongoClient mongoClient, String userName, String pwd) {
//		DB db = mongoClient.getDB(dbName);
//		authed = db.authenticate(userName, pwd.toCharArray());
//	}

	/**
	 * 获取db实例
	 * 
	 * @param key
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public DB getMongoDb() {
		// int num = Math.abs(key.hashCode());
		// long result = num % listMongoClient.size();
		// DB db = listMongoClient.get((int) result).getDB(dbName);
		DB db = mongoClient.getDB(dbName);
		return db;
	}

	/**
	 * 获取要处理的集合
	 * 
	 * @param dbkey
	 *            分库key
	 * @param collectionName
	 * @return
	 */
//	public DBCollection getCollection(String collectionName) {
//		DBCollection conn = null;
//		if (authed) {
//			conn = getMongoDb().getCollection(collectionName);
//		} else {
//			conn = null;
//			throw new RuntimeException("mongo auth error");
//		}
//		return conn;
//	}

	  public DBCollection getCollection(String collectionName) {
	      return getMongoDb().getCollection(collectionName);
	  }
	
	/**
	 * 构造mongo connection list
	 * ip:port:dbname:username:pwd|ip:dbname:username:port|ip:dbname:username:port
	 * 
	 * @param zkPath
	 * @return
	 */
	private List<ServerAddress> constructConnectionAddress(String ips) {
		List<ServerAddress> list = new ArrayList<ServerAddress>();
		// String mongoArrays[] = new String(zkHelp.getValue(zkPath)).split("[|]");
		String mongoArrays[] = ips.split("[|]");
		try {
			for (int i = 0; i < mongoArrays.length; i++) {
				String host_ports[] = mongoArrays[i].split(":");
				ServerAddress serverAddress = new ServerAddress(host_ports[0],
						Integer.parseInt(host_ports[1]));
				list.add(serverAddress);
				if (DefaultStringUtils.isEmpty(dbName) && DefaultStringUtils.isEmpty(pwd)) {
					this.dbName = host_ports[2];
					this.userName = host_ports[3];
					this.pwd = host_ports[4];
				}
			}
		} catch (Exception e) {
			logger.error("初始化mongo list出错" + Arrays.toString(mongoArrays), e);
			throw new RuntimeException("获取mongo配置串出错");
		}
		return list;
	}

	/**
	 * 释放资源
	 */
	public void releaseRs() {
		if (mongoClient != null)
			mongoClient.close();
		System.gc();
	}

	public static void main(String[] args) {
		String zkpath="";
		DBCollection collection = ShardDbPool.getInstance(zkpath).getCollection("users");
		DBObject dbo = null;
		for(int i = 0; i < 100000; i++) {
			dbo = new BasicDBObject();
			dbo.put("_id", i);
			dbo.put("i", i);
			dbo.put("name", "name" + i);
			dbo.put("cont", "垄断枯有在不顺困肖右； 顺不产晨顺晨虽顺嘎嘎顺嘎嘎" + i);
			dbo.put("time", System.currentTimeMillis());
			WriteResult result = collection.insert(dbo);
			System.out.println(result);
		}
	}

}
