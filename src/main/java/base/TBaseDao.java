package com.chanjet.imp.dao.mongo.base;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * mongodb 线程安全 单例数据库操作
 * 
 * @author haoxw
 * @since 2014/4/15
 */
public class TBaseDao {
	private final static Logger logger = LoggerFactory.getLogger(DbPool.class);
	private static TBaseDao instance = null;
	private DBCollection conn = null;
	private static DbPool dbPool = null;

	private TBaseDao() {

	}

	public synchronized static TBaseDao getInstance(String zkPath) {
		if (instance == null) {
			instance = new TBaseDao();
			dbPool = DbPool.getInstance(zkPath);
		}
		return instance;
	}

	/**
	 * 简单条件查找 比如 属性1=xxx 属性2=mmm
	 * 
	 * @param dbkey
	 * @param object
	 * @param collectionName
	 * @param className
	 * @return
	 */
	public <T> List<T> findByParams(String dbkey, BasicDBObject object,
			String collectionName, Class<T> clazz) {
		List<T> resultList = new ArrayList<T>();
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			List<DBObject> list = conn.find(object).toArray();
			for (DBObject dbObj : list) {
				T obj = db2Bean(dbObj, clazz);
				resultList.add(obj);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
		return resultList;

	}

	/**
	 * 复杂条件查找 大于 等于 正则等
	 * 
	 * @param dbkey
	 * @param dbObject
	 * @param collectionName
	 * @param className
	 * @return
	 */
	public <T> List<T> findByParamsDBObject(String dbkey, DBObject dbObject,
			String collectionName, Class<T> clazz) {

		List<T> resultList = new ArrayList<T>();
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			List<DBObject> list = conn.find(dbObject).toArray();
			for (DBObject dbObj : list) {
				T obj = db2Bean(dbObj, clazz);
				resultList.add(obj);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
		return resultList;

	}

	/**
	 * 原生mongo对象查询返回原生mongo列表
	 * 
	 * @param dbkey
	 * @param dbObject
	 * @param collectionName
	 * @return
	 */
	public List<DBObject> findByParamsDBObject2(String dbkey,
			DBObject dbObject, String collectionName) {
		List<DBObject> resultList = null;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			resultList = conn.find(dbObject).toArray();
		} catch (Exception e) {
			logger.error("", e);
		}
		return resultList;

	}

	// added by lijin,2014-4-25
	/**
	 * 复杂条件查找,增加limit和sort
	 * 
	 * @param dbkey
	 * @param dbObject
	 * @param collectionName
	 * @param limit
	 * @param orderBy
	 * @param className
	 * @return
	 */
	public <T> List<T> findByParamsDBObjectWithOrderLimit(String dbkey,
			DBObject dbObject, String collectionName, int limit,
			DBObject orderBy, Class<T> clazz) {

		List<T> resultList = new ArrayList<T>();
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			List<DBObject> list = conn.find(dbObject).sort(orderBy)
					.limit(limit).toArray();
			for (DBObject dbObj : list) {
				T obj = db2Bean(dbObj, clazz);
				resultList.add(obj);
			}
		} catch (Exception e) {
			logger.error("", e);
		}
		return resultList;
	}

	/**
	 * 复杂条件查找,增加limit和sort
	 * 
	 * @param dbkey
	 * @param dbObject
	 * @param collectionName
	 * @param limit
	 * @param orderBy
	 * @return
	 */
	public List<DBObject> findByParamsDBObjectWithOrderLimit(String dbkey,
			DBObject dbObject, String collectionName, int limit,
			DBObject orderBy) {
		List<DBObject> list = new ArrayList<DBObject>();
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			list = conn.find(dbObject).sort(orderBy).limit(limit).toArray();
		} catch (Exception e) {
			logger.error("", e);
		}
		return list;
	}

	// 2014-4-28
	public int updateMulti(String dbkey, BasicDBObject update,
			BasicDBObject where, String collectionName) {
		int result = -1;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			result = conn.updateMulti(where,
					new BasicDBObject().append("$set", update)).getN();
		} catch (Exception e) {
			logger.error("", e);
		}
		return result;
	}

	// end added

	/**
	 * 条件查找唯一记录
	 * 
	 * @param dbkey
	 * @param object
	 * @param collectionName
	 * @param className
	 * @return
	 */
	public <T> T findOne(String dbkey, BasicDBObject object,
			String collectionName, Class<T> clazz) {
		T obj = null;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			DBObject result = conn.findOne(object);
			obj = db2Bean(result, clazz);
		} catch (Exception e) {
			logger.error("", e);
		}
		return obj;
	}
	
	/**
	 * 条件查找唯一记录
	 * @param dbkey
	 * @param object
	 * @param collectionName
	 * @return
	 */
	public DBObject   findOne2(String dbkey,BasicDBObject object, String collectionName) {
		DBObject result = null;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			result = conn.findOne(object);
		} catch (Exception e) {
			logger.error("", e);
		}
		return result;
	}

	/**
	 * 根据主键查找
	 * 
	 * @param dbkey
	 * @param id
	 * @param collectionName
	 * @param className
	 * @return
	 */
	public <T> T findOneById(String dbkey, Object id, String collectionName,
			Class<T> clazz) {
		T obj = null;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			DBObject dbObject = conn.findOne(new BasicDBObject("_id", id));
			obj = db2Bean(dbObject, clazz);
		} catch (Exception e) {
			logger.error("", e);
		}
		return obj;
	}

	/**
	 * 添加或者修改记录
	 * 
	 * @param dbkey
	 * @param obj
	 * @param collectionName
	 * @return 如果结果大于-1 则表示添加成功
	 */
	public int addorupdate(String dbkey, Object obj, String collectionName) {
		int result = -1;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			DBObject dbObject = (DBObject) JSON.parse(JSONObject
					.fromObject(obj).toString());
			result = conn.save(dbObject).getN();
		} catch (Exception e) {
			logger.error("add", e);
		}
		return result;
	}

	/**
	 * 添加或者修改记录
	 * 
	 * @param dbkey
	 * @param obj
	 *            原生mongo对象
	 * @param collectionName
	 * @return 如果结果大于-1 则表示添加成功
	 */
	public int addorupdate(String dbkey, DBObject obj, String collectionName) {
		int result = -1;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			result = conn.save(obj).getN();
		} catch (Exception e) {
			logger.error("add", e);
		}
		return result;
	}

	/**
	 * drop collection
	 * 
	 * @param dbkey
	 * @param collectionName
	 * @return
	 */
	public void dropCollection(String dbkey, String collectionName) {
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			conn.drop();
		} catch (Exception e) {
			logger.error("dropCollection", e);
		}
	}

	/**
	 * 批量插入 这个慎用 因为一次插入是一个库和表 所以不能走分表逻辑
	 * 
	 * @param dbkey
	 * @param collectionName
	 * @param list
	 */
	public void addBatch(String dbkey, String collectionName, List<Object> list) {
		if (list == null || list.isEmpty()) {
			return;
		}
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			List<DBObject> listDB = new ArrayList<DBObject>();
			for (int i = 0; i < list.size(); i++) {
				listDB.add(bean2Db(list.get(i)));
			}
			conn.insert(listDB);
		} catch (Exception e) {
			logger.error("", e);
		}
	}

	/**
	 * 修改对象
	 * 
	 * @param dbkey
	 * @param newValue
	 * @param where
	 * @param collectionName
	 * @return
	 */
	public int update(String dbkey, Object newValue, BasicDBObject where,
			String collectionName) {
		int result = -1;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			result = conn.update(where, bean2Db(newValue)).getN();
		} catch (Exception e) {
			logger.error("", e);
		}
		return result;
	}

	/**
	 * 删除对象
	 * 
	 * @param dbkey
	 * @param where
	 *            不能空
	 * @param collectionName
	 * @return
	 */
	public int remove(String dbkey, BasicDBObject where, String collectionName) {
		int result = -1;
		if (where == null) {
			return result;
		}
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			result = conn.remove(where).getN();
		} catch (Exception e) {
			logger.error("", e);
		}
		return result;
	}

	/**
	 * 统计
	 * 
	 * @param dbkey
	 * @param where
	 * @param collectionName
	 * @return
	 */
	public int getCount(String dbkey, BasicDBObject where, String collectionName) {
		int result = 0;
		try {
			conn = dbPool.getCollection(dbkey, collectionName);
			result = conn.find(where).count();
		} catch (Exception e) {
			logger.error("", e);
		}
		return result;
	}
	/**
	 * 创建索引
	 * @param dbkey
	 * @param collectionName
	 * @param key 添加索引的key
	 * @param order 1升序 -1降序
	 */
	public void createIndex(String dbkey, String collectionName,String key,int order){
		try {
		conn = dbPool.getCollection(dbkey, collectionName);
		conn.createIndex(new BasicDBObject(key, order));
		} catch (Exception e) {
			logger.error("", e);
		}
	}
	/**
	 * mongo对象转换成普通java bean对象
	 * 
	 * @param dbObject
	 * @param className
	 * @return
	 * @throws Exception
	 */
	public <T> T db2Bean(DBObject dbObject, Class<T> clazz) throws Exception {
		if (dbObject == null) {
			return null;
		}
		T obj = clazz.newInstance();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			String fieldName = field.getName();
			String methodName = "set" + fieldName.substring(0, 1).toUpperCase()
					+ fieldName.substring(1, fieldName.length());
			Method method = null;
			Object resultObj = null;
			try {
				method = clazz.getMethod(methodName,
						new Class[] { field.getType() });
			} catch (Exception e) {
				logger.error("", e);
				continue;
			}
			resultObj = dbObject.get(fieldName);
			try {
				resultObj = method.invoke(obj, new Object[] { resultObj });
			} catch (Exception e) {
				logger.error("", e);
				continue;
			}
		}
		return obj;
	}

	public DBObject bean2Db(Object obj) throws Exception {
		if (obj == null) {
			return null;
		}
		DBObject dbObject = new BasicDBObject();
		Class<? extends Object> clazz = obj.getClass();
		Field[] fields = clazz.getDeclaredFields();
		for (Field field : fields) {
			String fieldName = field.getName();
			String methodName = "get" + fieldName.substring(0, 1).toUpperCase()
					+ fieldName.substring(1, fieldName.length());
			Method method = null;
			Object resultObj = null;
			try {
				method = clazz.getMethod(methodName);
			} catch (Exception e) {
				logger.error("", e);
				continue;
			}
			try {
				resultObj = method.invoke(obj);
			} catch (Exception e) {
				logger.error("", e);
				continue;
			}
			if (resultObj != null && !resultObj.equals("")) {
				dbObject.put(fieldName, resultObj);
			}
		}
		return dbObject;
	}

}
