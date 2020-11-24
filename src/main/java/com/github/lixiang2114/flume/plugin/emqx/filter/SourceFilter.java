package com.github.lixiang2114.flume.plugin.emqx.filter;

import java.util.Map;
import java.util.Properties;

/**
 * @author Louis(LiXiang)
 * @description Source过滤器接口
 */
public interface SourceFilter {
	/**
	 * @return
	 */
	public String[] getTopics();
	
	/**
	 * @param record
	 * @return
	 */
	public String doFilter(String record);
	
	/**
	 * @return
	 */
	default public int[] getQoses(){return null;}
	
	/**
	 * @return
	 */
	default public String getPassword(){return null;}
	
	/**
	 * @return
	 */
	default public String getUsername(){return null;}
	
	/**
	 * @param properties
	 */
	default public void filterConfig(Properties properties){}
	
	/**
	 * @param config
	 */
	default public void pluginConfig(Map<String,String> config){}
}
