/**
 * 
 */
package base.pgbase.model;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import base.pgbase.BaseDao;
import base.pgbase.RouteUtil;

/**
 * @author junxing.zhang
 * 
 *         2014年4月17日
 */
public class UserDao extends BaseDao {
	/**
	 * @param zkPath
	 */
	public UserDao(String zkPath) {
		super(zkPath);
	}

	private static RouteUtil routeUtil = RouteUtil.getInstance();

	public int save(long id) {
		String table = routeUtil.getTestTableName(id);
		String sql = "insert into " + table + "(userid) values(?)";
		ArrayList<Object> parameters = new ArrayList<Object>();
		parameters.add(id);
		long dbkey = id;
		return this.insert(dbkey, sql, parameters);
	}

	public User findById(long id) {
		long dbkey = id;
		String table = routeUtil.getTestTableName(id);
		String sql = "select * from " + table + " where userid=?";
		ArrayList<Long> parameters = new ArrayList<Long>();
		parameters.add(id);
		return (User) this.query(dbkey, sql, parameters, new CallBack() {
			@Override
			public Object getResultObject(ResultSet rs) {
				try {
					if (rs.next()) {
						User user = new User();
						user.setId(rs.getInt("userid"));
						user.setName(rs.getString("version"));
						return user;

					}
				} catch (SQLException e) {

				}
				return null;
			}
		});

	}

	public int removeById(long id) {
		String table = routeUtil.getTestTableName(id);
		String sql = "delete   from " + table + " where id=?";
		ArrayList<Long> parameters = new ArrayList<Long>();
		parameters.add(id);
		long dbkey = id;
		return this.delete(dbkey, sql, parameters);
	}

	public int updateNameById(long id, String name) {
		String table = routeUtil.getTestTableName(id);
		String sql = "update  " + table + " set name =? where id = ?";
		ArrayList<Object> parameters = new ArrayList<Object>();
		parameters.add("你好");
		parameters.add(id);
		long dbkey = id;
		return this.update(dbkey, sql, parameters);
	}

}
