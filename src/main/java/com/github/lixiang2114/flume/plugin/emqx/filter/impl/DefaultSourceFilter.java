package com.github.lixiang2114.flume.plugin.emqx.filter.impl;

import java.util.Map;

import com.github.lixiang2114.flume.plugin.emqx.filter.SourceFilter;

/**
 * @author Louis(LiXiang)
 * @description 默认Source过滤器实现
 */
public class DefaultSourceFilter implements SourceFilter{
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
	
	@Override
	public String[] getTopics() {
		return topics;
	}
	
	@Override
	public String getPassword() {
		return DefaultSourceFilter.passWord;
	}

	@Override
	public String getUsername() {
		return DefaultSourceFilter.userName;
	}

	@Override
	public String doFilter(String record) {
		return record;
	}

	@Override
	public void pluginConfig(Map<String, String> config) {
		String passWordStr=config.get("passWord");
		String userNameStr=config.get("userName");
		
		if(null!=passWordStr) {
			String pass=passWordStr.trim();
			if(0!=pass.length()) passWord=pass;
		}
		
		if(null!=userNameStr) {
			String user=userNameStr.trim();
			if(0!=user.length()) userName=user;
		}
	}
}
