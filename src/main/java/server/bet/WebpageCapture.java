package server.bet;


import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class WebpageCapture {
  public void captureHtml(String ip) throws Exception {
    String strURL = "http://ip.chinaz.com/?IP=" + ip;
    URL url = new URL(strURL);
    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    InputStreamReader input = new InputStreamReader(httpConn.getInputStream(), "utf-8");
    BufferedReader bufReader = new BufferedReader(input);
    String line = "";
    StringBuilder contentBuf = new StringBuilder();
    while ((line = bufReader.readLine()) != null) {
      contentBuf.append(line);
    }
    String buf = contentBuf.toString();
    System.out.println(buf);
    int beginIx = buf.indexOf("查询结果[");
    int endIx = buf.indexOf("IP", beginIx);
    String result = buf.substring(beginIx, endIx);
    System.out.println("captureHtml()的结果：\n" + result);
  }

  public void captureJavascript(String postid) throws Exception {
    String strURL = "http://www.kiees.cn/sf.php?wen=" + postid + "&channel=";
    URL url = new URL(strURL);
    HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
    InputStreamReader input = new InputStreamReader(httpConn.getInputStream(), "utf-8");
    BufferedReader bufReader = new BufferedReader(input);
    String line = "";
    StringBuilder contentBuf = new StringBuilder();
    while ((line = bufReader.readLine()) != null) {
      contentBuf.append(line);
    }
    System.out.println("captureJavascript()的结果：\n" + contentBuf.toString());
  }
}
