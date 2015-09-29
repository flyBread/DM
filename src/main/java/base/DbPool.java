package com.chanjet.imp.dao.mongo.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chanjet.imp.configcenter.ZkHelp;
import com.chanjet.imp.utils.DefaultStringUtils;
import com.github.zkclient.IZkDataListener;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

/**
 * mongodb 线程安全 单例池
 * 
 * @author haoxw
 * @since 2014/4/15
 */
public class DbPool {
  private final static Logger logger = LoggerFactory.getLogger(DbPool.class);

  private static ConcurrentMap<String, DbPool> map = new ConcurrentHashMap<String, DbPool>();

  // private static DbPool instance = null;
  public List<MongoClient> listMongoClient = null;
  private int poolSize = 40;
  private int blockSize = 5;
  // private boolean autoConnectRetry = true;
  private int connectTimeout = 3000;
  private int socketTimeout = 5000;
  private int maxWaitTime = 5000;
  public String dbName = "";
  public String userName = "";
  public String pwd = "";
  // public boolean authed = false;
  // private final static ZkHelp zkHelp = ZkHelp.getInstance();

  private DbPool() {}

  public synchronized static DbPool getInstance(String zkPath) {
    DbPool instance1 = map.get(zkPath);

    if (instance1 == null) {
      final DbPool instance = new DbPool();
      // IZkDataListener listener = new IZkDataListener() {
      // public void handleDataDeleted(String dataPath) throws Exception {
      // logger.info("!!! mongo node data has been deleted !!!" + dataPath);
      // }
      //
      // public void handleDataChange(String dataPath, byte[] data) throws
      // Exception {
      // logger.info("!!! mongo node data has been changed !!!" + dataPath);
      // String redisServerInf = DefaultStringUtils.toStr(data);
      // instance.initialPool(dataPath);
      // logger.info("!!! mongo node[" + redisServerInf + "] connection pool has
      // been rebuild !!!" + dataPath);
      // }
      // };
      // // 节点添加监控
      // zkHelp.subscribeDataChanges(zkPath, listener);
      // 初始化 公用redis集群
      instance.initialPool(zkPath);
      map.put(zkPath, instance);
    }
    // return instance;
    return map.get(zkPath);
  }

  /**
   * 初始化连接池，设置参数。 读zookeeper
   */
  public void initialPool(String zkPath) {
    logger.info("start init connection pools");
    // 如果不为null 先清理再建立
    if (null != listMongoClient && listMongoClient.size() > 0) {
      releaseRs();
    }
    listMongoClient = new ArrayList<MongoClient>();
    List<List<ServerAddress>> listServerAddresses = null;
    try {
      // mongodb连接串中间用空,开 ,多个实例用|分割
      // ip:port:dbname:userName:pwd,ip:port:dbname:userName:pwd,ip:port:dbname:userName:pwd|ip:port:dbname:userName:pwd,ip:port:dbname:userName:pwd,ip:port:dbname:userName:pwd
      listServerAddresses = constructConnectionAddress(zkPath);

      logger.info("list: {}", listServerAddresses);
      MongoCredential cr = MongoCredential.createCredential(userName, dbName, pwd.toCharArray());
      List<MongoCredential> list_mc = new ArrayList<MongoCredential>();
      list_mc.add(cr);

      for (int i = 0; i < listServerAddresses.size(); i++) {
        MongoClient mongoClient = null;
        // 其他参数根据实际情况进行添加
        MongoClientOptions mco = new MongoClientOptions.Builder()
            // .autoConnectRetry(autoConnectRetry)
            .writeConcern(WriteConcern.SAFE).connectionsPerHost(poolSize)
            .threadsAllowedToBlockForConnectionMultiplier(blockSize).connectTimeout(connectTimeout)
            .socketTimeout(socketTimeout).maxWaitTime(maxWaitTime).build();
        mongoClient = new MongoClient(listServerAddresses.get(i), list_mc, mco);
        // mongoClient = new MongoClient(listServerAddresses.get(i), mco);
        // authDb(mongoClient, userName, pwd);
        listMongoClient.add(mongoClient);
      }
      logger.info("end init connection pools");
    }
    catch (Exception e) {
      logger.error("构造连接池出错 {} ", listServerAddresses);
      logger.error("", e);
      e.printStackTrace();
    }
  }

  /**
   * 获取db容器
   * 
   * @param key
   * @return
   */
  // public void authDb(MongoClient mongoClient, String userName, String pwd) {
  // DB db = mongoClient.getDB(dbName);
  // authed = db.authenticate(userName, pwd.toCharArray());
  // }

  /**
   * 获取db实例
   * 
   * @param key
   * @return
   */
  @SuppressWarnings("deprecation")
  public DB getMongoDb(String key) {

    int num1 = key.hashCode();
    if (num1 == Integer.MIN_VALUE) {
      num1 = 0;
    }

    int num = Math.abs(num1);

    int total = listMongoClient.size();

    if (total == 0) {
      logger.error("mongodb 没有注册到zk {}", total);
      throw new RuntimeException("mongodb 没有注册到zk ");
    }

    long result = num % listMongoClient.size();
    DB db = listMongoClient.get((int) result).getDB(dbName);
    return db;
  }

  /**
   * 获取要处理的集合
   * 
   * @param dbkey
   *          分库key
   * @param collectionName
   * @return
   */
  // public DBCollection getCollection(String dbkey, String collectionName) {
  // DBCollection conn = null;
  // if (authed) {
  // conn = getMongoDb(dbkey).getCollection(collectionName);
  // } else {
  // conn = null;
  // throw new RuntimeException("mongo auth error");
  // }
  // return conn;
  // }

  public DBCollection getCollection(String dbkey, String collectionName) {
    return getMongoDb(dbkey).getCollection(collectionName);
  }

  /**
   * 获取要处理的集合
   * 
   * @param dbkey
   *          分库key
   * @param collectionName
   * @return
   */
  public MongoCollection<Document> getCollection1(String dbkey, String collectionName) {
    return getMongoDataBase(dbkey).getCollection(collectionName);
  }

  /**
   * 构造mongo connection list
   * 
   * @param zkPath
   * @return
   */
  private List<List<ServerAddress>> constructConnectionAddress(String zkPath) {
    List<List<ServerAddress>> listServerAddressBig = new ArrayList<List<ServerAddress>>();
    String mongoArrays[] = new String(zkHelp.getValue(zkPath)).split("[|]");
    try {
      for (int i = 0; i < mongoArrays.length; i++) {
        List<ServerAddress> listServerAddress = new ArrayList<ServerAddress>();
        String host_ports[] = mongoArrays[i].split(",");
        for (int j = 0; j < host_ports.length; j++) {
          String host_port[] = host_ports[j].split(":");
          ServerAddress serverAddress = new ServerAddress(host_port[0], Integer.parseInt(
              host_port[1]));
          listServerAddress.add(serverAddress);
          if (DefaultStringUtils.isEmpty(dbName) && DefaultStringUtils.isEmpty(pwd)) {
            this.dbName = host_port[2];
            this.userName = host_port[3];
            this.pwd = host_port[4];
          }
        }
        listServerAddressBig.add(listServerAddress);
      }
    }
    catch (Exception e) {
      logger.error("初始化mongo list出错 {}" + Arrays.toString(mongoArrays));
      logger.error("", e);
      throw new RuntimeException("获取mongo配置串出错");
    }
    return listServerAddressBig;
  }

  /**
   * 释放资源
   */
  public void releaseRs() {
    for (MongoClient mc : listMongoClient)
      if (mc != null) {
        mc.close();
        mc = null;
      }
    System.gc();
  }

  /**
   * 获取db实例
   * 
   * @param key
   * @return
   */
  public MongoDatabase getMongoDataBase(String key) {
    int num = Math.abs(key.hashCode());
    if (num == Integer.MIN_VALUE) {
      num = Integer.MAX_VALUE;
    }
    long result = num % listMongoClient.size();
    MongoDatabase db = listMongoClient.get((int) result).getDatabase(dbName);
    return db;
  }

  public static void main(String[] args) {
    String key = "gzq:comment:sort:5111061155328003838";
    int num = Math.abs(key.hashCode());
    long result = num % 3;
    System.out.println(result);
  }
}
