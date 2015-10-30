package base.mongoDBbase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import util.ClassTypeConversion;

/**
 * @author zhailzh1
 * 
 * @Date 2015年9月29日——上午11:22:38
 * 
 *       mongo数据库的操作工具类
 */
public class BaseDao {
  private final static Logger logger = LoggerFactory.getLogger(DbPool.class);
  private static ConcurrentMap<String, BaseDao> map = new ConcurrentHashMap<String, BaseDao>();

  private DbPool dbPool = null;

  private BaseDao(String configValue) {
    dbPool = DbPool.getInstance(configValue);
  }

  /**
   * 根据标记对应一个一个的baseDao的实例，这样不会在高并发的时候出现差错表 的bug
   */
  public synchronized static BaseDao getInstance(String zkPath) {
    BaseDao instance = map.get(zkPath);
    if (instance == null) {
      instance = new BaseDao(zkPath);
      map.put(zkPath, instance);
    }
    return instance;
  }

  /**
   * 保存对象
   * 
   * @param dbkey
   *          分库的依据
   * @param javaBean
   *          保存的对象
   * @param collectionName
   *          保存到的表名称
   */
  public int add(String dbkey, Object javaBean, String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      DBObject dbObject = ClassTypeConversion.objectToDbObject(javaBean);
      result = conn.save(dbObject).getN();
    }
    catch (Exception e) {
      logger.error("add", e);
    }
    return result;
  }

  /**
   * 删除对象
   * 
   * @param dbkey
   *          分库依据
   * @param where
   *          删除的条件
   * @param collectionName
   * @return
   */
  public Boolean remove(String dbkey, BasicDBObject where, String collectionName) {
    int result = -1;
    if (where == null) {
      return Boolean.FALSE;
    }
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.remove(where).getN();
    }
    catch (Exception e) {
      logger.error("", e);
      return Boolean.FALSE;
    }
    return result != -1;
  }

  /**
   * 
   * 
   * @param dbkey
   *          分库的依据
   * @param object
   *          查询的条件
   * @param collectionName
   *          所在的表名称
   * @param className
   *          查询对象的类名
   * @return
   */
  public List<Object> findByParams(String dbkey, String collectionName, BasicDBObject object,
      String className) {
    List<Object> resultList = new ArrayList<Object>();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      List<DBObject> list = conn.find(object).toArray();
      for (DBObject dbObj : list) {
        Object obj = ClassTypeConversion.db2Bean(dbObj, className);
        resultList.add(obj);
      }
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return resultList;

  }

  /**
   * 閫傜敤浜庨潪thrift鐢熸垚鐨刯avabean杩斿洖 澶嶆潅鏉′欢鏌ユ壘 澶т簬 绛変簬 姝ｅ垯绛�
   * 
   * @param dbkey
   * @param dbObject
   * @param collectionName
   * @param className
   * @return
   */
  public List<Object> findByParamsDBObject(String dbkey, DBObject dbObject, String collectionName,
      String className) {

    List<Object> resultList = new ArrayList<Object>();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      List<DBObject> list = conn.find(dbObject).toArray();
      for (DBObject dbObj : list) {
        Object obj = ClassTypeConversion.db2Bean(dbObj, className);
        resultList.add(obj);
      }
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return resultList;

  }

  /**
   * 鍘熺敓mongo瀵硅薄鏌ヨ杩斿洖鍘熺敓mongo鍒楄〃
   * 
   * @param dbkey
   * @param dbObject
   * @param collectionName
   * @return
   */
  public List<DBObject> findByParamsDBObject2(String dbkey, DBObject dbObject,
      String collectionName) {
    List<DBObject> list = null;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      list = conn.find(dbObject).toArray();
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return list;

  }

  // added by lijin,2014-4-25
  /**
   * 閫傜敤浜庨潪thrift鐢熸垚鐨刯avabean杩斿洖 澶嶆潅鏉′欢鏌ユ壘,澧炲姞limit鍜宻ort
   * 
   * @param dbkey
   * @param dbObject
   * @param collectionName
   * @param limit
   * @param orderBy
   * @param className
   * @return
   */
  public List<Object> findByParamsDBObjectWithOrderLimit(String dbkey, DBObject dbObject,
      String collectionName, int limit, DBObject orderBy, String className) {

    List<Object> resultList = new ArrayList<Object>();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      List<DBObject> list = conn.find(dbObject).sort(orderBy).limit(limit).toArray();
      for (DBObject dbObj : list) {
        Object obj = ClassTypeConversion.db2Bean(dbObj, className);
        resultList.add(obj);
      }
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return resultList;
  }

  // added by zhaoweih, 2014-5-16
  /**
   * 澶嶆潅鏉′欢鏌ユ壘,澧炲姞skip, limit鍜宻ort
   * 
   * @param dbkey
   * @param dbObject
   * @param collectionName
   * @param skip
   * @param limit
   * @param orderBy
   * @return List<DBObject>
   */
  public List<DBObject> findByParamsDBObjectWithOrderLimit(String dbkey, DBObject dbObject,
      String collectionName, int skip, int limit, DBObject orderBy) {

    List<DBObject> resultList = new ArrayList<DBObject>();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      resultList = conn.find(dbObject).sort(orderBy).limit(limit).skip(skip).toArray();
      // for (DBObject dbObj : list) {
      // Object obj = db2Bean(dbObj, className);
      // resultList.add(obj);
      // }
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return resultList;
  }

  // added by zhaoweih, 2014-5-16
  /**
   * 澶嶆潅鏉′欢鏌ユ壘,澧炲姞sort
   * 
   * @param dbkey
   * @param dbObject
   * @param collectionName
   * @param orderBy
   * @return List<DBObject>
   */
  public List<DBObject> findByParamsDBObjectWithOrderLimit(String dbkey, DBObject dbObject,
      String collectionName, DBObject orderBy) {

    List<DBObject> resultList = new ArrayList<DBObject>();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      resultList = conn.find(dbObject).sort(orderBy).toArray();
      // for (DBObject dbObj : list) {
      // Object obj = db2Bean(dbObj, className);
      // resultList.add(obj);
      // }
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return resultList;
  }

  // 2014-4-28
  public int updateMulti(String dbkey, BasicDBObject update, BasicDBObject where,
      String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.updateMulti(where, new BasicDBObject().append("$set", update)).getN();
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return result;
  }

  // end added

  /**
   * 閫傜敤浜庨潪thrift鐢熸垚鐨刯avabean杩斿洖 鏉′欢鏌ユ壘鍞竴璁板綍
   * 
   * @param dbkey
   * @param object
   * @param collectionName
   * @param className
   * @return
   */
  public Object findOne(String dbkey, BasicDBObject object, String collectionName,
      String className) {
    Object obj = null;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      DBObject result = conn.findOne(object);
      obj = ClassTypeConversion.db2Bean(result, className);
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return obj;
  }

  public DBObject findOne2(String dbkey, BasicDBObject object, String collectionName) {
    DBObject obj = null;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      obj = conn.findOne(object);
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return obj;
  }

  public Object findOneById(String dbkey, Object id, String collectionName, String className) {
    Object obj = null;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      DBObject dbObject = conn.findOne(new BasicDBObject("_id", id));
      obj = ClassTypeConversion.db2Bean(dbObject, className);
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return obj;
  }

  /**
   * 娣诲姞鎴栬�淇敼璁板綍
   * 
   * @param dbkey
   * @param obj
   *          鍘熺敓mongo瀵硅薄
   * @param collectionName
   * @return 濡傛灉缁撴灉澶т簬-1 鍒欒〃绀烘坊鍔犳垚鍔�
   */
  public int addorupdate(String dbkey, DBObject obj, String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.save(obj).getN();
    }
    catch (Exception e) {
      logger.error("add", e);
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.save(obj).getN();
    }
    return result;
  }

  /**
   * 鎵归噺鎻掑叆 杩欎釜鎱庣敤 鍥犱负涓�鎻掑叆鏄竴涓簱鍜岃〃 鎵�互涓嶈兘璧板垎琛ㄩ�杈�
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
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      List<DBObject> listDB = new ArrayList<DBObject>();
      for (int i = 0; i < list.size(); i++) {
        listDB.add(ClassTypeConversion.bean2Db(list.get(i)));
      }
      conn.insert(listDB);
    }
    catch (Exception e) {
      logger.error("", e);
    }
  }

  /**
   * 鎵归噺鎻掑叆 杩欎釜鎱庣敤 鍥犱负涓�鎻掑叆鏄竴涓簱鍜岃〃 鎵�互涓嶈兘璧板垎琛ㄩ�杈�
   * 
   * @param dbkey
   * @param collectionName
   * @param list
   */
  public void addBatchDbObject(String dbkey, String collectionName, List<DBObject> list) {
    if (list == null || list.isEmpty()) {
      return;
    }
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      conn.insert(list);
    }
    catch (Exception e) {
      logger.error("", e);
    }
  }

  /**
   * 淇敼瀵硅薄
   * 
   * @param dbkey
   * @param newValue
   * @param where
   * @param collectionName
   * @return
   */
  public int update(String dbkey, Object newValue, BasicDBObject where, String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.update(where, ClassTypeConversion.bean2Db(newValue)).getN();
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return result;
  }

  /**
   * 淇敼瀵硅薄
   * 
   * @param dbkey
   * @param newValue
   * @param where
   * @param collectionName
   * @return
   */
  public int update(String dbkey, BasicDBObject newValue, BasicDBObject where,
      String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.update(where, newValue).getN();
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return result;
  }

  /**
   * 淇敼瀵硅薄鐨勯儴鍒嗗瓧娈�
   * 
   * @param dbkey
   * @param newFieldValue
   * @param where
   * @param collectionName
   * @return
   */
  public int updateFields(String dbkey, BasicDBObject newFieldValue, BasicDBObject where,
      String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.update(where, new BasicDBObject("$set", newFieldValue), true, false).getN();
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return result;
  }

  /**
   * 淇敼瀵硅薄锛屽綋瀵硅薄涓嶅瓨鍦ㄧ殑鏃跺�鍒涘缓
   * 
   * @param dbkey
   * @param newValue
   * @param where
   * @param collectionName
   * @return
   */
  public int updateOrAdd(String dbkey, DBObject newValue, BasicDBObject where,
      String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.update(where, newValue, true, false).getN();
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return result;
  }

  /**
   * 缁熻
   * 
   * @param dbkey
   * @param where
   * @param collectionName
   * @return
   */
  public int getCount(String dbkey, BasicDBObject where, String collectionName) {
    int result = 0;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.find(where).count();
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return result;
  }

  /**
   * 鍒涘缓绱㈠紩
   * 
   * @param dbkey
   * @param collectionName
   * @param key
   *          娣诲姞绱㈠紩鐨刱ey
   * @param order
   *          1鍗囧簭 -1闄嶅簭
   */
  public void createIndex(String dbkey, String collectionName, String key, int order) {
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      conn.createIndex(new BasicDBObject(key, order));
    }
    catch (Exception e) {
      logger.error("", e);
    }
  }

  /**
   * 浠庝竴涓簱閲屾寜鏉′欢鍙栧�锛屽苟鎸夋潯浠舵帓搴忋�
   * 
   * @param dbKey
   * @param where
   *          鏉′欢
   * @param sort
   *          鎺掑簭
   * @param collectionName
   * @return
   */
  public List<DBObject> find(String dbKey, DBObject where, DBObject sort, String collectionName) {
    List<DBObject> list = null;
    try {
      DBCollection conn = dbPool.getCollection(dbKey, collectionName);
      list = conn.find(where).sort(sort).toArray();
    }
    catch (Exception ex) {
      logger.error("BaseDao find fail", ex);
    }
    return list;
  }

  public DbPool getDbPool() {
    return dbPool;
  }

  public void setDbPool(DbPool dbPool) {
    this.dbPool = dbPool;
  }

}
