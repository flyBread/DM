package rest.interceptor;

/**
 * @author zhailz
 *
 * @Date 2015年10月23日 下午3:05:14
 */
public class Context {

  public long userid; // required
  public String accesstoken; // required
  public String clientip; // optional
  public String appcode; // optional
  public String devicetype; // optional
  public String deviceid; // optional

  public long getUserid() {
    return userid;
  }

  public void setUserid(long userid) {
    this.userid = userid;
  }

  public String getAccesstoken() {
    return accesstoken;
  }

  public void setAccesstoken(String accesstoken) {
    this.accesstoken = accesstoken;
  }

  public String getClientip() {
    return clientip;
  }

  public void setClientip(String clientip) {
    this.clientip = clientip;
  }

  public String getAppcode() {
    return appcode;
  }

  public void setAppcode(String appcode) {
    this.appcode = appcode;
  }

  public String getDevicetype() {
    return devicetype;
  }

  public void setDevicetype(String devicetype) {
    this.devicetype = devicetype;
  }

  public String getDeviceid() {
    return deviceid;
  }

  public void setDeviceid(String deviceid) {
    this.deviceid = deviceid;
  }

}
