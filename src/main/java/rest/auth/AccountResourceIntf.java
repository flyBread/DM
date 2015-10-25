package rest.auth;

public interface AccountResourceIntf {

  String findToken(long userId, String deviceType, String deviceId);

  String getUserToken(long userId, String deviceType, String deviceId);

  boolean isCookieValid(String cookieValue);
}
