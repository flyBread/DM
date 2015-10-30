/**
 * 数据库表路由
 * 配置数据库表的路由配置信息
 */
package base.pgbase;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhangjxd
 * @since 2014/4/15
 */
public class RouteUtil {
	private static final RouteUtil instance = new RouteUtil();

	private RouteUtil() {
	}

	public synchronized static RouteUtil getInstance() {
		return instance;
	}

	public static final Map<Integer, String> TEST_TABLE_MAP = new HashMap<Integer, String>();
	static {
		TEST_TABLE_MAP.put(0, "systuser0");
	}

	public String getTestTableName(long key) {
		long i = key % TEST_TABLE_MAP.size();
		return TEST_TABLE_MAP.get((int) i);
	}
}
