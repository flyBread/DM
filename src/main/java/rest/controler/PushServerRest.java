package rest.controler;

import java.nio.charset.Charset;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@RequestMapping("/push/rest")
public class PushServerRest {

  private static final Logger logger = LoggerFactory.getLogger(PushServerRest.class);

  /**
   * js返回
   */
  public static final MediaType TEXT_JAVASCRIPT = new MediaType("text", "plain", Charset.forName(
      "UTF-8"));

  @RequestMapping(value = "/hello")
  @ResponseBody
  public ResponseEntity<String> getMsgsHistory(HttpServletRequest httpServletRequest) {
    logger.info("请求的路径是：{}", httpServletRequest.getRequestURI());
    ResponseEntity<String> result = new ResponseEntity<String>("helloworld", getResponseHeaders(),
        HttpStatus.OK);

    return result;
  }

  protected HttpHeaders getResponseHeaders() {
    HttpHeaders responseHeaders = new HttpHeaders();
    responseHeaders.setContentType(TEXT_JAVASCRIPT);
    return responseHeaders;
  }
}