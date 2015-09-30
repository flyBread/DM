package server.bet;

/**
 * @author zhailzh
 * 
 * @Date 2015年9月29日——下午4:05:00
 * 
 *       数据库保存的模型
 */
public class Model {

  private String date;
  private double prise;
  private String cate;
  public String getDate() {
    return date;
  }
  public void setDate(String date) {
    this.date = date;
  }
  public double getPrise() {
    return prise;
  }
  public void setPrise(double prise) {
    this.prise = prise;
  }
  public String getCate() {
    return cate;
  }
  public void setCate(String cate) {
    this.cate = cate;
  }
}
