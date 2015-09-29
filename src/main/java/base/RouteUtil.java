package com.chanjet.imp.dao.mongo.base;

import java.util.HashMap;
import java.util.Map;


/**
 * 路由计算表明 工具类 单例 线程安全
 * @author haoxw
 * @since 2014/4/15
 */
public class RouteUtil {
	private static final RouteUtil instance = new RouteUtil();
	private RouteUtil() {
	}
	public synchronized static RouteUtil getInstance() {
		return instance;
	}
	 /**
	  * 设置系统表
	  */
    public static final Map<Integer, String> PERSON_TABLE_MAP = new HashMap<Integer, String>();
    static {
    	PERSON_TABLE_MAP.put(0, "person");
    	PERSON_TABLE_MAP.put(1, "person1");
    	PERSON_TABLE_MAP.put(2, "person2");
    	PERSON_TABLE_MAP.put(3, "person3");
    }
	/**
	 * 按照worksId(资源id)对表的总个数取模、返回目标表明
	 * 
	 * @param sid
	 *            用户id
	 * @return 表名
	 */
	public String getPersonTableName(long key) {
		long i = key % PERSON_TABLE_MAP.size();
		// 获取表名
		return PERSON_TABLE_MAP.get((int)i);
	}

	public static void main(String a[]) {
		RouteUtil util = RouteUtil.getInstance();
		for (int i = 0; i < 10; i++) {
			String tablename = util.getPersonTableName( (long)i);
			System.out.println(tablename);
		}
	}
}
