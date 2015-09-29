package base;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

import net.sf.json.JSONObject;

/**
 * @author haoxw
 * @since 2014/4/15
 */
public class BaseDao {
  private final static Logger logger = LoggerFactory.getLogger(DbPool.class);
  private static ConcurrentMap<String, BaseDao> map = new ConcurrentHashMap<String, BaseDao>();

  private DbPool dbPool = null;

  private BaseDao(String zkPath) {
    dbPool = DbPool.getInstance(zkPath);
  }

  public synchronized static BaseDao getInstance(String zkPath) {

    /**
     * if (instance == null) { instance = new BaseDao(); dbPool =
     * DbPool.getInstance(zkPath); } return instance;
     **/

    BaseDao instance = map.get(zkPath);
    if (instance == null) {
      instance = new BaseDao(zkPath);
      map.put(zkPath, instance);
    }
    return instance;
  }

  /**
   * 閫傜敤浜庨潪thrift鐢熸垚鐨刯avabean杩斿洖 绠�崟鏉′欢鏌ユ壘 姣斿 灞炴�1=xxx 灞炴�2=mmm
   * 
   * @param dbkey
   * @param object
   * @param collectionName
   * @param className
   * @return
   */
  public List findByParams(String dbkey, BasicDBObject object, String collectionName,
      String className) {
    List resultList = new ArrayList();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      List<DBObject> list = conn.find(object).toArray();
      for (DBObject dbObj : list) {
        Object obj = db2Bean(dbObj, className);
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
  public List findByParamsDBObject(String dbkey, DBObject dbObject, String collectionName,
      String className) {

    List resultList = new ArrayList();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      List<DBObject> list = conn.find(dbObject).toArray();
      for (DBObject dbObj : list) {
        Object obj = db2Bean(dbObj, className);
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
  public List findByParamsDBObjectWithOrderLimit(String dbkey, DBObject dbObject,
      String collectionName, int limit, DBObject orderBy, String className) {

    List resultList = new ArrayList();
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      List<DBObject> list = conn.find(dbObject).sort(orderBy).limit(limit).toArray();
      for (DBObject dbObj : list) {
        Object obj = db2Bean(dbObj, className);
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

    List<DBObject> resultList = new ArrayList<>();
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

    List<DBObject> resultList = new ArrayList<>();
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
      obj = db2Bean(result, className);
    }
    catch (Exception e) {
      logger.error("", e);
    }
    return obj;
  }

  /**
   * 鏉′欢鏌ユ壘鍞竴璁板綍
   * 
   * @param dbkey
   * @param object
   * @param collectionName
   * @return
   */
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

  /**
   * 閫傜敤浜庨潪thrift鐢熸垚鐨刯avabean杩斿洖 鏍规嵁涓婚敭鏌ユ壘
   * 
   * @param dbkey
   * @param id
   * @param collectionName
   * @param className
   * @return
   */
  public Object findOneById(String dbkey, Object id, String collectionName, String className) {
    Object obj = null;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      DBObject dbObject = conn.findOne(new BasicDBObject("_id", id));
      obj = db2Bean(dbObject, className);
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
   * @param collectionName
   * @return 濡傛灉缁撴灉澶т簬-1 鍒欒〃绀烘坊鍔犳垚鍔�
   */
  @Deprecated
  public int addorupdate(String dbkey, Object obj, String collectionName) {
    int result = -1;
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      DBObject dbObject = (DBObject) JSON.parse(JSONObject.fromObject(obj).toString());
      result = conn.save(dbObject).getN();
    }
    catch (Exception e) {
      logger.error("add", e);
    }
    return result;
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
        listDB.add(bean2Db(list.get(i)));
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
      result = conn.update(where, bean2Db(newValue)).getN();
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
   * 鍒犻櫎瀵硅薄
   * 
   * @param dbkey
   * @param where
   *          涓嶈兘绌�
   * @param collectionName
   * @return
   */
  public int remove(String dbkey, BasicDBObject where, String collectionName) {
    int result = -1;
    if (where == null) {
      return result;
    }
    try {
      DBCollection conn = dbPool.getCollection(dbkey, collectionName);
      result = conn.remove(where).getN();
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
   * mongo瀵硅薄杞崲鎴愭櫘閫歫ava bean瀵硅薄
   * 
   * @param dbObject
   * @param className
   * @return
   * @throws Exception
   */
  public Object db2Bean(DBObject dbObject, String className) throws Exception {
    if (dbObject == null) {
      return null;
    }
    Class clazz = Class.forName(className);
    Object obj = clazz.newInstance();
    Field[] fields = clazz.getDeclaredFields();
    for (Field field : fields) {
      String fieldName = field.getName();
      String methodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1,
          fieldName.length());
      Method method = null;
      Object resultObj = null;
      try {
        method = clazz.getMethod(methodName, new Class[] { field.getType() });
      }
      catch (Exception e) {
        logger.error("", e);
        continue;
      }
      resultObj = dbObject.get(fieldName);
      try {
        resultObj = method.invoke(obj, new Object[] { resultObj });
      }
      catch (Exception e) {
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
      String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1,
          fieldName.length());
      Method method = null;
      Object resultObj = null;
      try {
        method = clazz.getMethod(methodName);
      }
      catch (Exception e) {
        logger.error("", e);
        continue;
      }
      try {
        resultObj = method.invoke(obj);
      }
      catch (Exception e) {
        logger.error("", e);
        continue;
      }
      if (resultObj != null && !resultObj.equals("")) {
        dbObject.put(fieldName, resultObj);
      }
    }
    return dbObject;
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
