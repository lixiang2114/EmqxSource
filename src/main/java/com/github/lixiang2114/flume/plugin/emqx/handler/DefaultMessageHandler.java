package com.github.lixiang2114.flume.plugin.emqx.handler;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.flume.Event;
import org.apache.flume.SystemClock;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.event.EventBuilder;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author Louis(LiXiang)
 * @description MQTT消息处理器
 */
public class DefaultMessageHandler implements MqttCallbackExtended{
	/**
	 * 批处理尺寸
	 */
	private Integer batchSize;
	
	/**
	 * 批处理超时时间
	 */
	private Long batchTimeout;
	
	/**
	 * 过滤方法
	 */
	private Method doFilter;
	
	/**
	 * 过滤器对象
	 */
	private Object filterObject;
	
	/**
	 * 批量剩余缓冲事件调度池句柄
	 */
	private ScheduledFuture<?> future;
	
	/**
	 * Flume通道处理器
	 */
	private ChannelProcessor channelProcessor;
	
	/**
	 * 批量剩余缓冲事件调度池
	 */
	private ScheduledExecutorService timedFlushService;
	
	/**
	 * 系统时钟
	 */
	private SystemClock systemClock = new SystemClock();
	
	/**
	 * 事件列表
	 */
	private List<Event> eventList = new ArrayList<Event>();
	
	/**
	 * 最后推入通道事件记录
	 */
	private Long lastPushToChannel = systemClock.currentTimeMillis();
	
	public DefaultMessageHandler(){
		this(100,30000L,null,null);
	}
	
	public DefaultMessageHandler(Integer batchSize,Long batchTimeout,Object filterObject,Method doFilter){
		this.doFilter=doFilter;
		this.batchSize=batchSize;
		this.filterObject=filterObject;
		this.batchTimeout=batchTimeout;
	}
	
	public void setDoFilter(Method doFilter) {
		this.doFilter = doFilter;
	}

	public void setFilterObject(Object filterObject) {
		this.filterObject = filterObject;
	}

	public void setChannelProcessor(ChannelProcessor channelProcessor) {
		this.channelProcessor = channelProcessor;
	}
	
	@Override
	public void connectComplete(boolean reconnect, String serverURI) {}
	
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {}
	
	@Override
	public void connectionLost(Throwable cause) {
		cause.printStackTrace();
	}

	/**
	 * 启动批量剩余缓冲事件调度池
	 */
	public void startBatchRemainFlushService(){
		ThreadFactory threadFactory=new ThreadFactoryBuilder().setNameFormat("timedFlushExecService"+Thread.currentThread().getId() + "-%d").build();
		timedFlushService = Executors.newSingleThreadScheduledExecutor(threadFactory);
		future = timedFlushService.scheduleWithFixedDelay(new Runnable() {
            public void run() {
              try {
            	  synchronized (eventList) {
            		  if (!eventList.isEmpty() && timeout()) flushEventBatch(eventList);
            	  }
              } catch (Exception e) {if (e instanceof InterruptedException) Thread.currentThread().interrupt();}
            }
        },batchTimeout, batchTimeout, TimeUnit.MILLISECONDS);
	}
	
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		String msg=new String(message.getPayload(),"UTF-8");
		String record=(String)doFilter.invoke(filterObject, msg);
		
		if(null==record || 0==record.trim().length()) return;
		
		synchronized (eventList) {
            eventList.add(EventBuilder.withBody(record,Charset.forName("UTF-8")));
            if (batchSize<=eventList.size() || timeout()) flushEventBatch(eventList);
        }
	}
	
	/**
	 * 停止处理器
	 */
	public void stop(){
		 if (future != null) future.cancel(true);
		 if (null == timedFlushService) return;
         timedFlushService.shutdown();
         while (!timedFlushService.isTerminated()) {
           try {
             timedFlushService.awaitTermination(500, TimeUnit.MILLISECONDS);
           } catch (InterruptedException e) {
             Thread.currentThread().interrupt();
           }
         }
	}
	
	/**
	 * @param eventList
	 */
	private void flushEventBatch(List<Event> eventList) {
		channelProcessor.processEventBatch(eventList);
		eventList.clear();
		lastPushToChannel = systemClock.currentTimeMillis();
    }

    /**
     * @return
     */
    private boolean timeout() {
    	return (systemClock.currentTimeMillis() - lastPushToChannel) >= batchTimeout;
    }
}
