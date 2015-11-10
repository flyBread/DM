package rest.adapter;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

import org.springframework.util.StringUtils;

/**
 * spring mvc httpservletrequest不会转义，这里把取参转义一下
 */
public class HttpServletPostRequestWapper extends HttpServletRequestWrapper {
  /**
   * Constructs a request object wrapping the given request.
   *
   * @param request
   *          httpservletrequest
   * @throws IllegalArgumentException
   *           if the request is null
   */
  public HttpServletPostRequestWapper(HttpServletRequest request) {
    super(request);
  }

  @Override
  public String getParameter(String name) {
    String value = super.getParameter(name);
    if (!StringUtils.isEmpty(value)) {
      try {
        return URLDecoder.decode(value, this.getCharacterEncoding());
      }
      catch (UnsupportedEncodingException e) {
        e.printStackTrace();
      }
      catch (IllegalArgumentException e) {
        e.printStackTrace();
        return value;
      }
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<String, String[]> getParameterMap() {
    Map<String, String[]> parameterMap = super.getParameterMap();
    Map<String, String[]> deCodeMap = new HashMap<String, String[]>();
    for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
      String[] values = entry.getValue();
      String[] decodeValues = new String[0];
      if (null != values && values.length != 0) {
        decodeValues = new String[values.length];
        for (int i = 0; i < values.length; i++) {
          try {
            decodeValues[i] = URLDecoder.decode(values[i], this.getCharacterEncoding());
          }
          catch (UnsupportedEncodingException e) {
            e.printStackTrace();
          }
          catch (IllegalArgumentException e1) {
            e1.printStackTrace();
            decodeValues[i] = values[i];
          }
        }
      }
      deCodeMap.put(entry.getKey(), decodeValues);
    }
    return deCodeMap;
  }

  /**
   * 打印http request 参数
   */
  public String httpParametersToString() {
    StringBuilder sb = new StringBuilder();
    Map<String, String[]> parameterMap = this.getParameterMap();
    for (Map.Entry<String, String[]> entry : parameterMap.entrySet()) {
      sb.append("{").append(entry.getKey()).append(":");
      for (String s : entry.getValue()) {
        sb.append(s).append(",");
      }
      sb.append("}; ");
    }
    return sb.toString();
  }
}
