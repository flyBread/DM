package base.pgbase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

//import com.chanjet.gongzuoquan.configcenter.ZkHelp;
//import com.github.zkclient.IZkDataListener;

/**
 * dbcp连接池 单例 线程安全
 * 
 * @author haoxw
 * @since 2014/5/12
 */
public class DbcpPool {
  private static DbcpPool instance = null;
  private final static Logger logger = LoggerFactory.getLogger(DbcpPool.class);
  // private final static ZkHelp zkHelp = ZkHelp.getInstance();
  private List<BasicDataSource> dataSource = null;
  private int maxActive = 200;// 最大连接数量
  private int maxIdle = 20;// 最大空闲连接
  private int minIdle = 20;// 最小空闲连接
  private int maxWait = 30000;// 超时等待时间以毫秒为单位 1000毫秒等于1秒
  private int initialSize = 10;// 初始化连接
  private boolean removeAbandoned = true;// 是否自动回收超时连接
  private int removeAbandonedTimeout = 120;// 超时时间(以秒数为单位)
  private boolean testOnBorrow = true;
  private boolean logAbandoned = true;// 是否在自动回收超时连接的时候打印连接的超时错误
  private boolean testWhileIdle = true;// testWhileIdle会定时校验numTestsPerEvictionRun个连接，只要发现连接失效，就将其移除再重新创建。
  private int numTestsPerEvictionRun = 200;//
  private long timeBetweenEvictionRunsMillis = 30000L;//
  private long minEvictableIdleTimeMillis = 1800000L;//

  private DbcpPool() {}

  /**
   * 获得实例
   * 
   * @param zkPath
   * @return
   */
  public synchronized static DbcpPool getInstance(String zkPath) {
    if (instance == null) {
      instance = new DbcpPool();

      // IZkDataListener listener = new IZkDataListener() {
      // public void handleDataDeleted(String zkPath) throws Exception {
      // logger.info("!!! pg node data has been deleted !!!"
      // + zkPath);
      // }
      //
      // public void handleDataChange(String dataPath, byte[] data)
      // throws Exception {
      //// logger.info("!!! pg node data has been changed !!!"
      //// + dataPath);
      //// String redisServerInf =
      // com.chanjet.gongzuoquan.utils.StringUtils.toStr(data);
      //// instance.initPools(dataPath);
      // logger.info("!!! pg node[ connection pool has been rebuild !!!"
      // + dataPath);
      // }
      // };
      // 节点添加监控
      // zkHelp.subscribeDataChanges(zkPath, listener);

      instance.initPools(zkPath);
    }
    return instance;
  }

  /**
   * 初始化连接池
   * 
   * @param zkPath
   */
  public void initPools(String zkPath) {
    logger.info("start init connection pools");
    dataSource = new ArrayList<BasicDataSource>();
    try {
      ArrayList<Map<String, String>> list = getAddress(zkPath);
      for (int i = 0; i < list.size(); i++) {
        BasicDataSource bds = new BasicDataSource();
        Map<String, String> addr = (Map<String, String>) list.get(i);

        bds = new BasicDataSource();
        bds.setDriverClassName("org.postgresql.Driver");
        bds.setUrl(addr.get("url").toString());
        bds.setUsername(addr.get("user").toString());
        bds.setPassword(addr.get("pwd").toString());

        bds.setMaxActive(this.maxActive);// 最大连接数量
        bds.setMaxIdle(this.maxIdle);// 最大空闲连接
        bds.setMinIdle(this.minIdle);// 最小空闲连接
        bds.setMaxWait(this.maxWait);// 超时等待时间以毫秒为单位 1000等于60秒
        bds.setInitialSize(this.initialSize);// 初始化连接
        bds.setRemoveAbandoned(this.removeAbandoned);
        bds.setRemoveAbandonedTimeout(this.removeAbandonedTimeout);
        bds.setTestOnBorrow(this.testOnBorrow);
        bds.setLogAbandoned(this.logAbandoned);

        bds.setTestWhileIdle(this.testWhileIdle);
        bds.setNumTestsPerEvictionRun(this.numTestsPerEvictionRun);
        bds.setTimeBetweenEvictionRunsMillis(this.timeBetweenEvictionRunsMillis);
        bds.setMinEvictableIdleTimeMillis(this.minEvictableIdleTimeMillis);
        dataSource.add(bds);
      }
      logger.info("end init connection pools");
    }
    catch (Exception e) {
      logger.error("init error", e);
    }
  }

  /**
   * 获得PG数据库配置
   * 
   * @param zkPath
   * @return ArrayList
   */
  private ArrayList<Map<String, String>> getAddress(String zkPath) {
    // String pgConfig = new String(zkHelp.getValue(zkPath));
    String pgConfig = new String(zkPath);

    if (StringUtils.isEmpty(pgConfig)) {
      throw new RuntimeException("系统异常，配置为空");
    }
    String pgArray[] = pgConfig.split(",");
    ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
    logger.info("Pg Config zkPath={},PgConfig={}", zkPath, pgConfig);
    for (int i = 0; i < pgArray.length; i++) {
      Map<String, String> addr = new HashMap<String, String>();
      String address[] = pgArray[i].split(":");
      addr.put("url", "dbc:postgresql://" + address[0] + ":" + address[1] + "/" + address[2]);
      addr.put("user", address[3]);
      addr.put("pwd", address[4]);
      list.add(addr);
    }
    return list;
  }

  /**
   * 按key取模分库
   * 
   * @param key
   * @return
   */
  public int getDbIndex(long key) {
    long result = key % dataSource.size();
    return (int) result;
  }

  /** 从数据源获得一个连接 */
  public Connection getConn(long key) {

    Connection conn = null;
    try {
      int index = getDbIndex(key);
      BasicDataSource bds = dataSource.get(index);
      conn = bds.getConnection();
    }
    catch (SQLException e) {
      // TODO Auto-generated catch block
      logger.error("getConn error", e);
    }
    return conn;
  }

  /**
   * 获得数据源连接状态
   * 
   * @param key
   * @return
   */
  public Map<String, Integer> getDataSourceStats(long key) {
    BasicDataSource bds = null;
    try {
      int index = getDbIndex(key);
      bds = dataSource.get(index);
    }
    catch (Exception e) {
      // TODO Auto-generated catch block
      logger.error("getDataSourceStats error", e);
    }
    Map<String, Integer> map = new HashMap<String, Integer>(2);
    map.put("active_number", (null == bds ? 0 : bds.getNumActive()));
    map.put("idle_number", (null == bds ? 0 : bds.getNumIdle()));
    return map;
  }

  /**
   * 关闭数据源
   * 
   * @param key
   * @throws SQLException
   */
  protected void shutdownDataSource(long key) throws SQLException {
    BasicDataSource bds = null;
    try {
      int index = getDbIndex(key);
      bds = dataSource.get(index);
      bds.close();
    }
    catch (Exception e) {
      // TODO Auto-generated catch block
      logger.error("shutdownDataSource error", e);
    }

  }

  /**
   * 关闭执行查询操作的连接
   * 
   * @param resultSet
   * @param statement
   * @param connection
   */
  public void closeConnection(ResultSet resultSet, Statement statement, Connection connection) {
    try {
      if (resultSet != null) {
        resultSet.close();
        resultSet = null;
      }
      if (statement != null) {
        statement.close();
        statement = null;
      }
      if (connection != null) {
        connection.close();
        connection = null;
      }
    }
    catch (SQLException e) {
      logger.error("closeConnection error", e);
    }
  }
}