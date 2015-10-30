package com.chanjet.gongzuoquan.dao.pg;

import org.junit.Test;

import base.pgbase.model.User;
import base.pgbase.model.UserDao;




/**
 * db操作测试用例
 * 
 * @author zhangjxd
 * @since 2014/4/15
 * 
 */
public class DaoTest {
	@Test
	public void test() {
		UserDao u = new UserDao("/gongzuoquan/account/pgs");
		long id = 1;
		User user = u.findById(id);
		System.out.println(user);
		if(user != null){
			System.out.println(user.getId());
			System.out.println(user.getName());
		}
		System.out.println(u.updateNameById(id, "張虎"));
		System.out.println(u.save(1));
		System.out.println(u.removeById(id));
	}
}
