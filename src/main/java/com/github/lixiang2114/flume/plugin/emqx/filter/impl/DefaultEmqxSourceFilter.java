package com.github.lixiang2114.flume.plugin.emqx.filter.impl;

import java.util.Map;

import com.github.lixiang2114.flume.plugin.emqx.filter.EmqxSourceFilter;
import com.github.lixiang2114.flume.plugin.emqx.util.TypeUtil;

/**
 * @author Louis(LiXiang)
 * @description 默认Source过滤器实现
 */
public class DefaultEmqxSourceFilter implements EmqxSourceFilter{
	/**
	 * 通信质量表
	 */
	private static int[] qoses;
	
	/**
	 * 连接主题列表
	 */
	private static String[] topics;
	
	/**
	 * 登录Emqx密码
	 */
	private static String passWord;
	
	/**
	 * 登录Emqx用户名
	 */
	private static String userName;
	
	/**
	 * 登录验证Token的秘钥
	 */
	private static String jwtSecret;
	
	/**
	 * 携带Token的字段名
	 */
	private static String tokenFrom;
	
	/**
	 * Token过期时间
	 */
	private static Integer tokenExpire;
	
	/**
	 * Token过期时间因数
	 */
	private static Integer expireFactor;
	
	@Override
	public int[] getQoses() {
		return qoses;
	}
	
	@Override
	public String[] getTopics() {
		return topics;
	}
	
	@Override
	public String getPassword() {
		return passWord;
	}

	@Override
	public String getUsername() {
		return userName;
	}
	
	@Override
	public String getJwtsecret() {
		return jwtSecret;
	}
	
	@Override
	public String getTokenfrom() {
		return tokenFrom;
	}

	@Override
	public Integer getTokenexpire() {
		return tokenExpire;
	}
	
	@Override
	public Integer getExpirefactor() {
		return expireFactor;
	}

	@Override
	public String[] doFilter(String record) {
		return new String[]{record};
	}

	@Override
	public void pluginConfig(Map<String, String> config) {
		String qosesStr=config.get("qoses");
		String topicsStr=config.get("topics");
		String jwtSecretStr=config.get("jwtSecret");
		String passWordStr=config.get("passWord");
		String userNameStr=config.get("userName");
		String tokenFromStr=config.get("tokenFrom");
		String tokenExpireStr=config.get("tokenExpire");
		String expireFactorStr=config.get("expireFactor");
		
		if(null!=qosesStr) {
			String qos=qosesStr.trim();
			if(0!=qos.length()) qoses=TypeUtil.toType(qos,int[].class);
		}
		
		if(null!=topicsStr) {
			String topic=topicsStr.trim();
			if(0!=topic.length()) topics=TypeUtil.toType(topic,String[].class);
		}
		
		if(null!=jwtSecretStr) {
			String secret=jwtSecretStr.trim();
			if(0!=secret.length()) jwtSecret=secret;
		}
		
		if(null!=passWordStr) {
			String pass=passWordStr.trim();
			if(0!=pass.length()) passWord=pass;
		}
		
		if(null!=userNameStr) {
			String user=userNameStr.trim();
			if(0!=user.length()) userName=user;
		}
		
		if(null!=tokenFromStr) {
			String from=tokenFromStr.trim();
			if(0!=from.length()) tokenFrom=from;
		}
		
		if(null!=tokenExpireStr) {
			String expire=tokenExpireStr.trim();
			if(0!=expire.length()) tokenExpire=Integer.parseInt(expire);
		}
		
		if(null!=expireFactorStr) {
			String factor=expireFactorStr.trim();
			if(0!=factor.length()) expireFactor=Integer.parseInt(factor);
		}
	}
}
