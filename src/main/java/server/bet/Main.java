package server.bet;

import java.util.Date;

import base.BaseDao;
import configCenter.Configuration;

public class Main {
  public static void main(String[] args) {
    String value = Configuration.getSignalIns().getConfigValue("/base/mongoValue");
    BaseDao dao = BaseDao.getInstance(value);
    Model model = new Model();
    model.setCate("date");
    model.setDate(new Date().toString());
    model.setPrise(17.30);
    dao.add("1", model, model.getClass().getSimpleName());
  }
}
