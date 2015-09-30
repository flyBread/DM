package util;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

/**
 * @author zhailzh
 * 
 * @Date 2015年9月30日——上午10:05:19
 * 
 */
public class ClassTypeConversion {

  private static Logger logger = LoggerFactory.getLogger(ClassTypeConversion.class);

  // 有javabean的对象，转化为DBObject对象，可以进一步的优化
  public static DBObject objectToDbObject(Object obj) {
    String jsonString = new JSONObject(obj).toString();
    DBObject dbObject = (DBObject) JSON.parse(jsonString);
    return dbObject;
  }

  /**
   * mongo DBObject的对象，根据 ClassName转化为 javaBean 对象
   * 
   * @param dbObject
   * @param className
   * @return
   * @throws Exception
   */
  public static Object db2Bean(DBObject dbObject, String className) throws Exception {
    if (dbObject == null) {
      return null;
    }
    Class<?> clazz = Class.forName(className);
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

  /**
   * 规范的javaBean对象，转化为Mongo的DBObject
   * 
   */
  public static DBObject bean2Db(Object obj) throws Exception {
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
}