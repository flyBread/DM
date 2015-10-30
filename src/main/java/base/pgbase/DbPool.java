/**
 * Postgresql DBCP连接池
 */
package base.pgbase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import com.chanjet.imp.configcenter.ZkHelp;

/**
 * postgresql 线程安全 单例池
 * 
 * @author zhangjxd
 * @since 2014/4/15
 */
public class DbPool {
	private final static Logger logger = LoggerFactory.getLogger(DbPool.class);
	private static DbPool instance = null;

//	private final static ZkHelp zkHelp = ZkHelp.getInstance();
	private BasicDataSource bds;
	private List<BasicDataSource> dataSource = null;
	private int maxActive = 200;// 最大连接数量
	private int maxIdle = 20;// 最大空闲连接
	private int minIdle = 20;// 最小空闲连接
	private int maxWait = 30000;// 超时等待时间以毫秒为单位 1000毫秒等于1秒
	private int initialSize = 10;// 初始化连接
	private boolean removeAbandoned = true;// 是否自动回收超时连接
	private int removeAbandonedTimeout = 120;// 超时时间(以秒数为单位)
	private boolean testOnBorrow = true; //在取出连接时进行有效验证
	private boolean logAbandoned = true;// 是否在自动回收超时连接的时候打印连接的超时错误
	private boolean testWhileIdle = true;//testWhileIdle会定时校验numTestsPerEvictionRun个连接，只要发现连接失效，就将其移除再重新创建。
	private int numTestsPerEvictionRun =200;//
	private long timeBetweenEvictionRunsMillis = 30000L;//
	private long minEvictableIdleTimeMillis = 1800000L;//
	private DbPool() {
	}

	/**
	 * 获得实例
	 * 
	 * @param zkPath
	 * @return
	 */
	public synchronized static DbPool getInstance(String zkPath) {
		if (instance == null) {
			instance = new DbPool();
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
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
		}
	}

	/**
	 * 获得PG数据库配置
	 * 
	 * @param zkPath
	 * @return ArrayList
	 */
	private ArrayList<Map<String, String>> getAddress(String zkPath) {
//		String pgConfig = new String(zkHelp.getValue(zkPath));
		String pgConfig = new String(zkPath);

		String pgArray[] = pgConfig.split(",");
		ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
		logger.info("Pg Config zkPath={},PgConfig={}", zkPath, pgConfig);
		for (int i = 0; i < pgArray.length; i++) {
			Map<String, String> addr = new HashMap<String, String>();
			String address[] = pgArray[i].split(":");
			addr.put("url", "dbc:postgresql://" + address[0] + ":" + address[1]
					+ "/" + address[2]);
			addr.put("user", address[3]);
			addr.put("pwd", address[4]);
			list.add(addr);
		}
		return list;
	}

	/**
	 * 获取pg连接实例
	 * 
	 * @param key
	 * @return
	 * @throws SQLException
	 */
	public Connection getConnection(long key) throws SQLException {
		int index = this.getDbIndex(key);
		BasicDataSource bds = dataSource.get(index);
		logger.info("bds.getNumActive()="+bds.getNumActive()+"   bds.getNumIdle()="+bds.getNumIdle());
		return bds.getConnection();
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

	/**
	 * 设置数据提交模式
	 * 
	 * @param key
	 * @param autoCommit
	 * @throws SQLException
	 */
	public void setAutoCommit(long key, boolean autoCommit) throws SQLException {
		getConnection(key).setAutoCommit(autoCommit);

	}

	/**
	 * 提交数据
	 * 
	 * @param key
	 * @throws SQLException
	 */
	public void commit(long key) throws SQLException {
		getConnection(key).commit();
	}

	/**
	 * 释放数据库连接资源
	 * 
	 * @param conn
	 *            Connection
	 * @param st
	 *            PreparedStatement
	 * @param rs
	 *            ResultSet
	 */
	public void free(Connection conn, PreparedStatement st, ResultSet rs) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
			} finally {
				if (st != null) {
					try {
						st.close();
					} catch (SQLException e) {
					} finally {
						if (conn != null) {
							try {
								conn.close();
							} catch (SQLException e) {
							}
						}
					}
				}
			}
		}
	}
}
