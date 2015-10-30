package base.sqlbase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import base.pgbase.DbPool;
import base.pgbase.model.CallBack;

/**
 * @author zhailzh
 * 
 * @Date 2015年10月26日——上午10:50:02
 * 
 */
public class BaseDao {
	private final static Logger logger = LoggerFactory.getLogger(BaseDao.class);
	private static DbPool dbPool = null;
	
	public BaseDao(String zkPath) {
		dbPool = DbPool.getInstance(zkPath);
	}

	/**
	 * 查询数据 返回一条或多条结果集
	 * 
	 * @param dbkey
	 *            按照此key作为分库的依据
	 * @param sql
	 *            prepare sql
	 * @param parameters
	 *            占位符参数
	 * @param callBack
	 *            回调函数
	 * @return Object
	 */
	@SuppressWarnings("rawtypes")
	public Object query(long dbkey, String sql, ArrayList parameters,
			CallBack callBack) {
		return executeQuery(dbkey, sql, parameters, callBack);
	}

	/**
	 * 刪除数据
	 * 
	 * @param dbkey
	 *            按照此key作为分库的依据
	 * @param sql
	 *            prepare sql
	 * @param parameters
	 *            占位符参数
	 * @return int
	 */
	@SuppressWarnings("rawtypes")
	public int delete(long dbkey, String sql, ArrayList parameters) {
		return executeUpdate(dbkey, sql, parameters);
	}

	/**
	 * 修改数据
	 * 
	 * @param dbkey
	 *            按照此key作为分库的依据
	 * @param sql
	 *            prepare sql
	 * @param parameters
	 *            占位符参数
	 * @return int
	 */
	@SuppressWarnings("rawtypes")
	public int update(long dbkey, String sql, ArrayList parameters) {
		return executeUpdate(dbkey, sql, parameters);
	}

	/**
	 * 插入数据
	 * 
	 * @param dbkey
	 *            按照此key作为分库的依据
	 * @param sql
	 *            prepare sql
	 * @param parameters
	 *            占位符参数
	 * @return int
	 */
	@SuppressWarnings("rawtypes")
	public int insert(long dbkey, String sql, ArrayList parameters) {
		return executeUpdate(dbkey, sql, parameters);
	}

	/**
	 * 执行操作数据
	 * 
	 * @param dbkey
	 *            按照此key作为分库的依据
	 * @param sql
	 *            prepare sql
	 * @param parameters
	 *            占位符参数
	 * @return int
	 */
	@SuppressWarnings("rawtypes")
	public int executeUpdate(long dbkey, String sql, ArrayList parameters) {
		logger.info("start executeUpdate");
		Connection conn = null;
		PreparedStatement smt = null;
		ResultSet rs = null;

		int num = 0;
		try {
			int dbindex = dbPool.getDbIndex(dbkey);
			logger.info("execute sql={},params={},dbkey={}", sql,
			    (null == parameters ? "" : parameters.toString()), dbkey);

			conn = dbPool.getConnection(dbindex);
			smt = conn.prepareStatement(sql);
			if (parameters != null) {
				for (int i = 0; i < parameters.size(); i++) {
					smt.setObject(i + 1, parameters.get(i));
				}
			}
			num = smt.executeUpdate();
			logger.info("execute result={}", num);
		} catch (SQLException e) {
			logger.error(e.getMessage(),e);
			throw new RuntimeException(e.getMessage());
		} finally {
			dbPool.free(conn, smt, rs);
			logger.info("end executeUpdate");
		}
		return num;
	}

	/**
	 * 查询数据 返回一条或多条结果集
	 * 
	 * @param dbkey
	 *            按照此key作为分库的依据
	 * @param sql
	 *            prepare sql
	 * @param parameters
	 *            占位符参数
	 * @param callBack
	 *            回调函数
	 * @return Object
	 */
	@SuppressWarnings({ "rawtypes" })
	public Object executeQuery(long dbkey, String sql, ArrayList parameters,
			CallBack callBack) {
		logger.info("start executeQuery");
		Connection conn = null;
		PreparedStatement smt = null;
		ResultSet rs = null;
		try {
			int dbindex = dbPool.getDbIndex(dbkey);
			logger.info("execute sql={},params={},dbkey={}", sql,
			    (null == parameters ? "" : parameters.toString()), dbkey);

			conn = dbPool.getConnection(dbindex);
			smt = conn.prepareStatement(sql);
			if (parameters != null) {
				for (int i = 0; i < parameters.size(); i++) {
					smt.setObject(i + 1, parameters.get(i));
				}
			}
			rs = smt.executeQuery();
			logger.info("execute success");
			return callBack.getResultObject(rs);
		} catch (SQLException e) {
			logger.error(e.getMessage(),e);
			throw new RuntimeException(e.getMessage());
		} finally {
			dbPool.free(conn, smt, rs);
			logger.info("end executeQuery");
		}
	}

}

