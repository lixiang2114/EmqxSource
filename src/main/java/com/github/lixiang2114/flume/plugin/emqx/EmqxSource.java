package com.github.lixiang2114.flume.plugin.emqx;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttClientPersistence;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.github.lixiang2114.flume.plugin.emqx.handler.DefaultMessageHandler;
import com.github.lixiang2114.flume.plugin.emqx.handler.TokenExpireHandler;
import com.github.lixiang2114.flume.plugin.emqx.util.ClassLoaderUtil;
import com.github.lixiang2114.flume.plugin.emqx.util.TokenUtil;
import com.github.lixiang2114.flume.plugin.emqx.util.TypeUtil;

/**
 * @author Louis(LiXiang)
 * @description Emqx-Source插件
 */
public class EmqxSource extends AbstractSource implements EventDrivenSource,Configurable ,BatchSizeSupported {
	/**
	 * Qos列表
	 */
	private static int[] qoses;
	
	/**
	 * 主题列表
	 */
	private static String[] topics;
	
	/**
	 * 主机列表
	 */
	private static String[] hostList;
	
	/**
	 * 批处理尺寸
	 */
	private static Integer batchSize;
	
	/**
	 * 批处理超时时间
	 */
	private static Long batchTimeout;
	
	/**
	 * 过滤器名称
	 */
	private static String filterName;
	
	/**
	 * 过滤器对象
	 */
	private static Object filterObject;
	
	/**
	 * Mqtt客户端
	 */
	private static MqttClient mqttClient;
	
	/**
	 * 是否需要启动Token过期调度器
	 */
	private static Boolean startTokenScheduler;
	
	/**
	 * 是否使用密码字段携带Token
	 */
	private static boolean tokenFromPass=true;
	
	/**
	 * Mqtt客户端持久化模式
	 */
	private static MqttClientPersistence persistence;
	
	/**
	 * Mqtt客户端连接参数
	 */
	private static MqttConnectOptions mqttConnectOptions;
	
	/**
	 * Mqtt消息回调处理器
	 */
	private static MqttCallbackExtended mqttCallbackExtended;
	
	/**
	 * 英文冒号正则式
	 */
	private static final Pattern COLON_REGEX=Pattern.compile(":");
	
	/**
	 * 英文逗号正则式
	 */
	private static final Pattern COMMA_REGEX=Pattern.compile(",");
	
	/**
     * 数字正则式
     */
	public static final Pattern NUMBER_REGEX=Pattern.compile("^[0-9]+$");
	
	/**
     * IP地址正则式
     */
	public static final Pattern IP_REGEX=Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	
	/**
	 * Source默认过滤器
	 */
	private static final String DEFAULT_FILTER="com.github.lixiang2114.flume.plugin.emqx.filter.impl.DefaultEmqxSourceFilter";
	
	@Override
	public synchronized void start() {
		super.start();
		if(null!=startTokenScheduler && startTokenScheduler)TokenExpireHandler.startTokenScheduler(mqttConnectOptions, tokenFromPass);
		DefaultMessageHandler messageHandler=(DefaultMessageHandler)mqttCallbackExtended;
		try {
			messageHandler.setChannelProcessor(getChannelProcessor());
			messageHandler.startBatchRemainFlushService();
			mqttClient.subscribe(topics, qoses);
		} catch (MqttException e) {
			e.printStackTrace();
		}
	}

	@Override
	public synchronized void stop() {
		try{
			((DefaultMessageHandler)mqttCallbackExtended).stop();
			TokenExpireHandler.stopTokenScheduler();
			mqttClient.disconnectForcibly();
			mqttClient.close(true);
		}catch(MqttException e){
			e.printStackTrace();
		}
		super.stop();
	}

	@Override
	public long getBatchSize() {
		return batchSize;
	}

	@Override
	public void configure(Context context) {
		//获取上下文参数1:过滤器名称
		filterName=getParamValue(context,"filterName", "filter");
		
		//获取上下文参数2:批处理尺寸
		batchSize=Integer.parseInt(getParamValue(context,"batchSize", "100"));
		
		//获取上下文参数3:批处理超时时间
		batchTimeout=Long.parseLong(getParamValue(context,"batchTimeout", "3000"));
		
		//装载自定义过滤器类路径
		try {
			addFilterClassPath();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		
		//装载过滤器配置
		Properties filterProperties=new Properties();
		try{
			System.out.println("INFO:====load filter config file:"+filterName+".properties");
			InputStream inStream=ClassLoaderUtil.getClassPathFileStream(filterName+".properties");
			filterProperties.load(inStream);
		}catch(Exception e){
			e.printStackTrace();
		}
		
		//获取绑定的过滤器类
		Class<?> filterType=null;
		try {
			String filterClass=(String)filterProperties.remove("type");
			if(null==filterClass || 0==filterClass.trim().length()){
				filterClass=DEFAULT_FILTER;
				System.out.println("WARN:filterName=="+filterName+" the filter is empty or not found, the default filter will be used...");
			}
			System.out.println("INFO:====load filter class file:"+filterClass);
			filterType=Class.forName(filterClass);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
		
		//创建过滤器对象
		try {
			filterObject=filterType.newInstance();
		} catch (InstantiationException | IllegalAccessException e1) {
			throw new RuntimeException("Error:filter object instance failure!!!");
		}
		
		//回调初始化插件参数
		try {
			Method pluginConfig = filterType.getDeclaredMethod("pluginConfig",Map.class);
			if(null!=pluginConfig) pluginConfig.invoke(filterObject, context.getParameters());
		} catch (Exception e) {
			System.out.println("Warn: "+filterType.getName()+" may not be initialized:contextConfig");
		}
		
		//自动初始化过滤器参数
		try {
			initFilter(filterType,filterProperties);
		} catch (ClassNotFoundException | IOException e) {
			System.out.println("Warn: "+filterType.getName()+" may not be auto initialized:filterConfig");
		}
		
		//回调初始化过滤器参数
		try {
			Method filterConfig = filterType.getDeclaredMethod("filterConfig",Properties.class);
			if(null!=filterConfig) filterConfig.invoke(filterObject, filterProperties);
		} catch (Exception e) {
			System.out.println("Warn: "+filterType.getName()+" may not be manual initialized:filterConfig");
		}
		
		//获取上下文参数3:Emqx主机地址
		initHostAddress(context);
		if(null==hostList) throw new RuntimeException("Error:host address can not be NULL or EMPTY!!!");
		
		//获取上下文参数4:Mqtt主机连接参数
		initMqttClientOptions(context,filterType);
		
		//初始化过滤器对象与接口表
		initFilterFace(filterType);
	}
	
	/**
	 * @param context
	 */
	private static final void initHostAddress(Context context){
		//获取连接协议类型
		String protocolType=getParamValue(context,"protocolType", "tcp")+"://";
		
		//计算默认端口号
		String defaultPort="ssl://".equals(protocolType)?"8883":"1883";
		
		//获取主机列表字串
		String tmpHostStr=context.getString("hostList","").trim();
		String hostStr=0!=tmpHostStr.length()?tmpHostStr:"127.0.0.1:"+defaultPort;
		
		//初始化主机列表
		ArrayList<String> tmpList=new ArrayList<String>();
		String[] hosts=COMMA_REGEX.split(hostStr);
		for(int i=0;i<hosts.length;i++){
			String host=hosts[i].trim();
			if(0==host.length()) continue;
			String[] ipAndPort=COLON_REGEX.split(host);
			if(ipAndPort.length>=2){
				String ip=ipAndPort[0].trim();
				String port=ipAndPort[1].trim();
				if(!IP_REGEX.matcher(ip).matches()) continue;
				if(!NUMBER_REGEX.matcher(port).matches()) continue;
				tmpList.add(new StringBuilder(protocolType).append(ip).append(":").append(port).toString());
				continue;
			}
			
			if(ipAndPort.length<=0) continue;
			
			String unknow=ipAndPort[0].trim();
			if(NUMBER_REGEX.matcher(unknow).matches()){
				tmpList.add(new StringBuilder(protocolType).append("127.0.0.1:").append(unknow).toString());
			}else if(IP_REGEX.matcher(unknow).matches()){
				tmpList.add(new StringBuilder(protocolType).append(unknow).append(":").append(defaultPort).toString());
			}
		}
		
		int hostCount=tmpList.size();
		if(0!=hostCount) hostList=tmpList.toArray(new String[hostCount]);
	}
	
	/**
	 * @param context
	 */
	private static final void initMqttClientOptions(Context context,Class<?> filterType){
		mqttConnectOptions = new MqttConnectOptions();
		
		mqttConnectOptions.setServerURIs(hostList);
		mqttConnectOptions.setMaxInflight(Integer.parseInt(getParamValue(context,"automaticReconnect", "10")));
		mqttConnectOptions.setCleanSession(Boolean.parseBoolean(getParamValue(context,"cleanSession", "true")));
		mqttConnectOptions.setKeepAliveInterval(Integer.parseInt(getParamValue(context,"keepAliveInterval", "60")));
		mqttConnectOptions.setConnectionTimeout(Integer.parseInt(getParamValue(context,"connectionTimeout", "30")));
		mqttConnectOptions.setAutomaticReconnect(Boolean.parseBoolean(getParamValue(context,"automaticReconnect", "true")));
		
		String jwtSecret=null;
		try{
			jwtSecret=(String)filterType.getDeclaredMethod("getJwtsecret").invoke(filterObject);
		}catch(Exception e){
			System.out.println("Warn:===jwt secret information not found...");
		}
		
		String userName=null;
		String passWord=null;
		try{
			userName=(String)filterType.getDeclaredMethod("getUsername").invoke(filterObject);
			passWord=(String)filterType.getDeclaredMethod("getPassword").invoke(filterObject);
		}catch(Exception e){
			System.out.println("Warn:===username or password information not found...");
		}
		
		passWord=null==passWord||0==passWord.trim().length()?"public":passWord.trim();
		userName=null==userName||0==userName.trim().length()?"admin":userName.trim();
			
		if(null!=jwtSecret){
			Integer tokenExpire=null;
			try{
				tokenExpire=(Integer)filterType.getDeclaredMethod("getTokenexpire").invoke(filterObject);
			}catch(Exception e){
				System.out.println("Warn:===token expire information not found...");
			}
			
			Integer expireFactor=null;
			try{
				expireFactor=(Integer)filterType.getDeclaredMethod("getExpirefactor").invoke(filterObject);
			}catch(Exception e){
				System.out.println("Warn:===expire factor information not found...");
			}
			
			if(null==tokenExpire) tokenExpire=3600;
			if(null==expireFactor) expireFactor=750;
			
			if(-1==tokenExpire.intValue()) {
				expireFactor=1000;
				tokenExpire=1000000000;
				startTokenScheduler=false;
			}
			
			String token=null;
			try {
				token=TokenUtil.initToken(jwtSecret, tokenExpire, userName, expireFactor);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			
			if(null==token) throw new RuntimeException("token is NULL or EMPTY!!!");
			
			String tokenFromField=null;
			try{
				tokenFromField=(String)filterType.getDeclaredMethod("getTokenfrom").invoke(filterObject);
			}catch(Exception e){
				System.out.println("Warn:===token field name is unknow,default use password...");
			}
			
			if(null==tokenFromField) tokenFromField="password";
			
			if("username".equalsIgnoreCase(tokenFromField)) {
				userName=token;
				tokenFromPass=false;
			}else {
				passWord=token;
				tokenFromPass=true;
			}
			
			if(null==startTokenScheduler) startTokenScheduler=true;
		}
		
		mqttConnectOptions.setUserName(userName);
		mqttConnectOptions.setPassword(passWord.toCharArray());
		
		String persistenceType=getParamValue(context,"persistenceType", "org.eclipse.paho.client.mqttv3.persist.MemoryPersistence");
		try {
			persistence=(MqttClientPersistence)Class.forName(persistenceType).newInstance();
		} catch (Exception e) {
			persistence=new MemoryPersistence();
			System.out.println("WARN:==="+persistenceType+" is not be found,default use MemoryPersistence...");
		}
	}
	
	/**
	 * @param filterType
	 */
	private static final void initFilterFace(Class<?> filterType) {
		ArrayList<String> list=new ArrayList<String>();
		try{
			String[] tmpTopics=(String[])filterType.getDeclaredMethod("getTopics").invoke(filterObject);
			if(null==tmpTopics || 0==tmpTopics.length) throw new RuntimeException("topics can not be NULL or EMPTY!!!");
			for(String iTopic:tmpTopics) {
				if(null==iTopic) continue;
				String topic=iTopic.trim();
				if(0==topic.length()) continue;
				list.add(topic);
			}
		}catch(Exception e){
			throw new RuntimeException(e);
		}
		
		if(0==list.size()) throw new RuntimeException("topics can not be NULL or EMPTY!!!");
		topics=list.toArray(new String[list.size()]);
		qoses=new int[topics.length];
		
		int[] tmpQoses=null;
		try{
			tmpQoses=(int[])filterType.getDeclaredMethod("getQoses").invoke(filterObject);
		}catch(Exception e){
			System.out.println("WARN:===getQoses method can not be found,use default Qos=1...");
		}
		
		int qosLen=qoses.length;
		if(null==tmpQoses) {
			for(int i=0;i<qosLen;qoses[i++]=1);
		}else{
			int i=0;
			int tmpQosLen=tmpQoses.length;
			for(;i<qosLen&&i<tmpQosLen;i++) qoses[i]=tmpQoses[i];
			for(;i<qosLen;qoses[i++]=1);
		}
		
		Method doFilter=null;
		 try {
			doFilter=filterType.getDeclaredMethod("doFilter",String.class);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
		 
		if(null==doFilter) new RuntimeException("ERROR:===doFilter method can not be found....");
		
		try {
			String clientId=MqttClient.generateClientId();
			mqttClient=new MqttClient(mqttConnectOptions.getServerURIs()[0],clientId,persistence);
			mqttCallbackExtended=new DefaultMessageHandler(batchSize,batchTimeout,filterObject,doFilter);
			mqttClient.setCallback(mqttCallbackExtended);
			mqttClient.connectWithResult(mqttConnectOptions);
		} catch (MqttException e) {
			 throw new RuntimeException(e);
		}
	}
	
	/**
	 * @param filterType
	 * @param filterProperties
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private static final void initFilter(Class<?> filterType,Properties filterProperties) throws IOException, ClassNotFoundException {
		if(null==filterType || 0==filterProperties.size()) return;
		for(Map.Entry<Object, Object> entry:filterProperties.entrySet()){
			String key=((String)entry.getKey()).trim();
			if(0==key.length()) continue;
			Field field=null;
			try {
				field=filterType.getDeclaredField(key);
				field.setAccessible(true);
			} catch (NoSuchFieldException | SecurityException e) {
				e.printStackTrace();
			}
			
			if(null==field) continue;
			
			Object value=null;
			try{
				value=TypeUtil.toType((String)entry.getValue(), field.getType());
			}catch(RuntimeException e){
				e.printStackTrace();
			}
			
			if(null==value) continue;
			
			try {
				if((field.getModifiers() & 0x00000008) == 0){
					field.set(filterObject, value);
				}else{
					field.set(filterType, value);
				}
			} catch (IllegalArgumentException | IllegalAccessException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @throws URISyntaxException
	 * @throws IOException
	 */
	private static final void addFilterClassPath() throws URISyntaxException, IOException{
		File file = new File(new File(EmqxSource.class.getResource("/").toURI()).getParentFile(),"filter");
		if(!file.exists()) file.mkdirs();
		ClassLoaderUtil.addFileToCurrentClassPath(file, EmqxSource.class);
	}
	
	/**
	 * @param context
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	private static final String getParamValue(Context context,String key,String defaultValue){
		String value=context.getString(key,defaultValue).trim();
		return value.length()==0?defaultValue:value;
	}
}
