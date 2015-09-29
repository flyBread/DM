package server.bet;


public class Main {
  public static void main(String[] args) {
    WebpageCapture demo = new WebpageCapture();
    try {
      demo.captureHtml("111.142.55.73");
      demo.captureJavascript("107818590577");
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
