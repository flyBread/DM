package rest.interceptor;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import rest.auth.BaseAuthInterceptor;

public class AuthInterceptor extends BaseAuthInterceptor {
  static {
    useUriFilterForNoCheck();
    controllerUriFilter.add("/push/hello");
    // controllerUriFilter.add("/user/rest/authByToken");
    // controllerUriFilter.add("/user/rest/authByToken2");
    // controllerUriFilter.add("/imp_user_rest/user/rest/getUserInfo");
  }

  /**
   * 最后执行，可用于释放资源
   */
  @Override
  public void afterCompletion(HttpServletRequest request, HttpServletResponse response,
      Object handler, Exception ex) throws Exception {
    super.afterCompletion(request, response, handler, ex);
    // ContextUtils.clean();
  }

  /**
   * Controller之前执行
   */
  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    String requestURI = request.getRequestURI();
    boolean isOk = true;
    if (isOk/* ConfigUtils.isCheckAnth() */) {
      isOk = super.preHandle(request, response, handler);
      if (isOk && !controllerUriFilter.contains(requestURI)) {
        initContext(request);
      }
    }
    return isOk;
  }

  public void writeResponse(HttpServletResponse response) throws IOException {
    super.writeResponse(response, 1024/* "imp-code" */, "msg");
  }

  /**
   * 初始化context，保存客户端信息
   * 
   * @param request
   */
  private void initContext(HttpServletRequest request) {
    // Context context = new Context();
    // context.setUserid( );
    // context.setAccesstoken( );
    // context.setDevicetype( );
    // context.setDeviceid(CookiesUtil.getInstance().getUserDeviceId(request));
    // ContextUtils.set(context);
  }
}