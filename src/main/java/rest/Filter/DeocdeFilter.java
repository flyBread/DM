package rest.Filter;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rest.adapter.HttpServletPostRequestWapper;

public class DeocdeFilter implements Filter {

  private final static Logger log = LoggerFactory.getLogger(DeocdeFilter.class);

  public void init(FilterConfig filterConfig) throws ServletException {

  }

  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {
    HttpServletRequest httpServletRequest = (HttpServletRequest) request;
    String method = httpServletRequest.getMethod();
    log.info("httprequest method is --> {}", method);
    if (method.toLowerCase().equals("post")) {
      HttpServletPostRequestWapper requestWapper = new HttpServletPostRequestWapper(
          httpServletRequest);
      log.info("httprequest parameter are -->  {}", requestWapper.httpParametersToString());
      chain.doFilter(requestWapper, response);
    } else {
      chain.doFilter(request, response);
    }
  }

  public void destroy() {

  }
}
