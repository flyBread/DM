package configCenter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

/**
 * @author zhailzh
 * 
 * @Date 2015年9月29日——上午11:22:38
 * 
 */
public class Configuration {

  private Properties proper = null;

  private Configuration() {
    // 加载配置的文件
    proper = new Properties();
    InputStream inStream = this.getClass().getResourceAsStream("config.properties");
    try {
      proper.load(inStream);
    }
    catch (IOException e) {
      e.printStackTrace();
      throw new IllegalArgumentException("配置文件不存在，只能使用默认的配置信息。");
    }
  }

  private static class SignalHolder {
    private static Configuration ins = new Configuration();
  }

  public static Configuration getSignalIns() {
    return SignalHolder.ins;
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}