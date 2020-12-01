### 插件开发背景
EmqxSource是Flume流处理工具下基于Emqx消息中间件的一款Sink插件，截止目前的Flume1.9版本发布包，还不曾携带基于Emqx中间件的Source插件，为实现Flume流处理工具能够很好的对接到Emqx中间件服务，特开发EmqxSource插件，该插件基于Mqttv3驱动1.2.2版本开发，即：org.eclipse.paho.client.mqttv3-1.2.2


​      
### 插件功能特性
1. 版本无关性  
EmqxSource插件目前被设计成依赖Mqttv3版本（对于V5版本则需要升级驱动程序包）事件驱动类Source插件，截止在设计该插件时的Emqx版本为4.2.1，Mqttv3版本驱动足以支撑其通信能力，Mqtt协议是建立在TCP协议层之上的应用层协议，它广泛应用于互联网领域的消息互联  

2. 插件扩展性  
这是一款Flume-Source插件，它除了基于默认配置来完成一些简单的基础过滤功能，还提供了基于JAVA语言自定义的过滤器扩展，使用者可以根据自己的业务定制编写自己的个性化过滤器并将其放置到Flume安装目录下的filter目录中，同时配置好使用自定义过滤器，该插件即可回调自定义过滤器完成日志记录的过滤操作    
   
   ​     

### 插件使用说明
#### Flume工具及插件安装
1. 下载JDK-1.8.271  
wget https://download.oracle.com/otn/java/jdk/8u271-b09/61ae65e088624f5aaa0b1d2d801acb16/jdk-8u271-linux-x64.tar.gz  
  
    
   
2. 安装JDK-1.8.271  
tar -zxvf jdk-8u271-linux-x64.tar.gz -C /software/jdk1.8.0_271  
echo -e "JAVA_HOME=/software/jdk1.8.0_271\nPATH=$PATH:$JAVA_HOME/lib:$JAVA_HOME/bin\nexport PATH JAVA_HOME">>/etc/profile && source /etc/profile  
  
    
   
3. 下载Flume-1.9.0  
wget https://github.com/lixiang2114/Software/raw/main/flume-1.9.0.zip
  
    
   
4. 安装Flume-1.9.0  
unzip flume-1.9.0.zip -d /software/  
   
    
   
5. 下载插件EmqxSource-1.0  
wget https://github.com/lixiang2114/EmqxSource/raw/main/depends.zip  
  
    
   
6. 安装插件EmqxSource-1.0  
unzip depends.zip   &&   cp -a depends/*   /software/flume-1.9.0/lib/  
   
    
   
#### Emqx服务安装
1. 下载Emqx-4.2.1  
wget https://github.com/lixiang2114/Software/raw/main/emqx4.2.1.zip  
   
    
   
2. 安装Emqx-4.2.1  
unzip emqx4.2.1.zip -d /software/  
   
    
   

说明：    
若搭建Emqx分布式集群，则请运行"emqx_ctl cluster join nodename"命令追加集群中的节点，推荐构建分布式集群，因为在消息流较大时可以有效提升吞吐量，消息量不大时不建议构建成分布式集群，因为此时的瓶颈将发生在节点之间的网络通信阻力上    


​      
#### EmqxSource插件基础使用
**  Note：**下面以抽取日志为例来说明插件的基本使用方法    
1. 编写Flume任务流程配置  
```Text
vi /software/flume-1.9.0/process/conf/example01.conf
a1.sources=s1
a1.sinks=k1
a1.channels=c1

a1.sources.s1.persistenceType=org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
a1.sources.s1.type=com.github.lixiang2114.flume.plugin.emqx.EmqxSource
a1.sources.s1.hostList=192.168.162.127:1883
a1.sources.s1.jwtSecret=bGl4aWFuZw==
a1.sources.s1.filterName=emqxFilter
a1.sources.s1.tokenFrom=password
a1.sources.s1.batchTimeout=3000
a1.sources.s1.protocolType=tcp
a1.sources.s1.tokenExpire=-1
a1.sources.s1.userName=admin
a1.sources.s1.batchSize=100
a1.sources.s1.channels=c1

a1.sinks.k1.type=logger
a1.sinks.k1.channel=c1

a1.channels.c1.type=memory
a1.channels.c1.capacity=1000
a1.channels.c1.transactionCapacity=100
```


3. 启动Emqx服务  
```Shell
/software/emqx4.2.1/bin/emqx start
lsof -i tcp:1883
```


4. 启动Flume服务  
```Shell
/software/flume-1.9.0/bin/flume-ng agent -c /software/flume-1.9.0/conf -f /software/flume-1.9.0/process/conf/example03.conf -n a1 -Dflume.root.logger=INFO,console
```
备注：  
现在可以使用JAVA客户端推送消息到Emqx服务了，观察Flume控制台的日志输出即可验证下次数据是否已经通过Emqx服务转发  
    
    
#### EmqxSource插件过滤器使用  
##### 过滤器接口规范简介
不同的Source组件可以对应到不同的插件过滤器，编写插件过滤器的接口规范如下：  
```JAVA
package com.github.lixiang2114.flume.plugin.emqx.filter;

import java.util.Map;
import java.util.Properties;

/**
 * @author Louis(LiXiang)
 * @description SourceFilter过滤器接口规范
 */
public interface EmqxSourceFilter {
	/**
	 * 获取主题列表(必须重写)
	 * @return 主题列表
	 */
	public String[] getTopics();
	
	/**
	 * 日志数据如何通过过滤转换成文档记录(必须重写)
	 * @param record
	 * @return 文档记录
	 */
	public String[] doFilter(String record);
	
	/**
	 * 日志数据的通信质量列表
	 * @return 质量列表
	 */
	default public int[] getQoses(){return null;}
	
	/**
	 * 登录Emqx服务的密码(如果需要则重写)
	 * @return 登录密码
	 */
	default public String getPassword(){return null;}
	
	/**
	 * 登录Emqx服务的用户名(如果需要则重写)
	 * @return 登录用户
	 */
	default public String getUsername(){return null;}
	
	/**
	 * 生成登录Token的秘钥(如果需要则重写)
	 * @return Token秘钥
	 */
	default public String getJwtsecret(){return null;}
	
	/**
	 * Token的有效期系数/因子(如果需要则重写)
	 * @return 有效期系数
	 */
	default public Integer getExpirefactor(){return 750;}
	
	/**
	 * Token的过期时间(如果需要则重写)
	 * @return 过期时间
	 */
	default public Integer getTokenexpire(){return 3600;}
	
	/**
	 * 使用过滤器配置初始化本过滤器实例成员变量(如果需要则重写)
	 * @param properties 配置
	 */
	default public void filterConfig(Properties properties){}
	
	/**
	 * Token通过哪个字段携带到Emqx服务端
	 * @return 携带字段
	 */
	default public String getTokenfrom(){return "password";}
	
	/**
	 * 使用Flume插件配置初始化本过滤器实例成员变量(如果需要则重写)
	 * @param config 配置
	 */
	default public void pluginConfig(Map<String,String> config){}
}

```
说明：  
编写插件过滤器通常需要实现SourceFilter接口，但这并不是必须的，考虑到程序员编码的灵活性，EmqxSource插件被设计成约定优于配置的原则，因此程序员只需要在自定义的过滤器实现类中提供相应的接口规范即可，EmqxSource总是可以根据接口规范检索到对应的接口签名并正确无误的去回调它   


​    
##### 自定义过滤器实现步骤  
1. 编写过滤器实现类  
```JAVA
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import com.github.lixiang2114.flume.plugin.emqx.filter.EmqxSourceFilter;

/**
 * @author Louis(LiXiang)
 * @description 自定义日志过滤器
 */
public class EmqxLoggerFilter implements EmqxSourceFilter{
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
```

说明：  
上面实现的接口EmqxSourceFilter来自于FlumePluginFilter.jar包，我们可以从github上下载获得：
wget https://github.com/lixiang2114/Document/raw/main/plugin/flume1.9/face/FlumePluginFilter.jar  
可以使用Eclipse、Idea等IDE集成开发工具来完成上述编码和编译过程，如果过滤器项目是基于Maven构建的，还可以直接使用Maven来编译项目，如果过滤器简单到只有单个类文件也可以直接使用命令行编译：  
javac -cp FlumePluginFilter.jar EmqxLoggerFilter.java  

如果编译后的项目不止一个字节码文件则需要打包：  
Maven： mvn package -f  /xxx/pom.xml  
JAVA：jar -cvf xxx.jar -C \[project\]  


​    
2. 发布过滤器  
* 发布过滤器代码  
不论过滤器项目编译后是单个字节码文件还是压缩打成的jar包，我们都可以直接将其拷贝到filter目录下的lib子目录中即可：  
cp -a EmqxLoggerFilter.class /software/flume-1.9.0/filter/lib/  
或  
cp -a EmqxLoggerFilter.jar /software/flume-1.9.0/filter/lib/  
  
    
  
* 配置发布的过滤器  
```Text
vi /software/flume-1.9.0/filter/emqxFilter.properties  
type=EmqxLoggerFilter
qoses=Test-Topics
```


说明：  
因为上述的EmqxLoggerFilter非常简单，就是一个字节码文件，没有定义包名（即存在于类路径下的默认包中），所以看到的就是一个类名，如果过滤器的入口类（实现SourceFilter接口的类）有包名则必须带上包名  

经过以上步骤之后，我们启动Flume服务，EmqxSource插件就会自动调动我们自定义的过滤器类EmqxLoggerFilter来完成日志过滤处理了  




##### 过滤器高级应用  
EmqxSource插件支持多实例Source复用，即不同的Source实例可以重用EmqxSource插件，假如我们有两个Emqx的集群构建，那么我们可以在Flume的任务流程配置中配置好两个不同的Source实例，这两个Source实例中的数据分别被推送到不同的通道Channel，同时为两个不同的Source实例指定不同的过滤器参数名（使用参数名filterName指定，默认提供的filterName参数值是filter）：    
      
```Text
a1.sources.s1.persistenceType=org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
a1.sources.s1.type=com.github.lixiang2114.flume.plugin.emqx.EmqxSource
a1.sources.s1.hostList=192.168.162.127:1883
a1.sources.s1.jwtSecret=bGl4aWFuZw==
a1.sources.s1.filterName=filter01
a1.sources.s1.tokenFrom=password
a1.sources.s1.batchTimeout=3000
a1.sources.s1.protocolType=tcp
a1.sources.s1.tokenExpire=-1
a1.sources.s1.userName=admin
a1.sources.s1.batchSize=100
a1.sources.s1.channels=c1 

a1.sources.s1.persistenceType=org.eclipse.paho.client.mqttv3.persist.MemoryPersistence
a1.sources.s1.type=com.github.lixiang2114.flume.plugin.emqx.EmqxSource
a1.sources.s1.hostList=192.168.162.128:1883
a1.sources.s1.jwtSecret=bGl4aWFuZw==
a1.sources.s1.filterName=filter02
a1.sources.s1.tokenFrom=password
a1.sources.s1.batchTimeout=3000
a1.sources.s1.protocolType=tcp
a1.sources.s1.tokenExpire=-1
a1.sources.s1.userName=admin
a1.sources.s1.batchSize=100
a1.sources.s1.channels=c2 
```

然后在filter目录下指定对应的过滤器配置文件即可（根据约定优于配置的原则，我们定义的文件名需要与filterName参数值保持相同，比如默认文件名为：filter.properties），一个典型的过滤器配置形如下面给出的格式：    
    
```Text
cat filter01.properties
type=UserInfoFilter
qoses=Test-Topics1    
    
        
cat filter02.properties
type=OrderInfoFilter
qoses=Test-Topics2      
```

最后还需要分别编写过滤器类UserInfoFilter和OrderInfoFilter，注意上面定义的这两个类都没有包名，这说明它们被放在默认的classpath的类路径根目录下，为了便于简化程序员的编码和部署工作，EmqxSource插件允许对一些非常简单的过滤操作只需要编写一个单类即可，编译好这个单类并将它拷贝到filter目录下即完成快捷部署。当然如果对于一些过滤非常复杂的操作（比如在过滤中涉及到一些业务逻辑的处理等），我们也可以启动一个完整的JAVA工程或Maven工程来编写过滤器，最后将其打包成jar文件拷贝到filter目录下，** 过滤器的编写参见上述章节的讲解 **    
    
程序员在自定义过滤器实现的过程中，其过滤器类中成员变量名应该与过滤器配置文件中的参数名保持一致，这将有利于EmqxSource插件自动化初始化类的成员，同时在过滤器规范中有有以下接口是可选的实现：    

```JAVA
/**
* 日志数据的通信质量列表
* @return 质量列表
*/
default public int[] getQoses(){return null;}

/**
* 登录Emqx服务的密码(如果需要则重写)
* @return 登录密码
*/
default public String getPassword(){return null;}

/**
* 登录Emqx服务的用户名(如果需要则重写)
* @return 登录用户
*/
default public String getUsername(){return null;}

/**
* 生成登录Token的秘钥(如果需要则重写)
* @return Token秘钥
*/
default public String getJwtsecret(){return null;}

/**
* Token的有效期系数/因子(如果需要则重写)
* @return 有效期系数
*/
default public Integer getExpirefactor(){return 750;}

/**
* Token的过期时间(如果需要则重写)
* @return 过期时间
*/
default public Integer getTokenexpire(){return 3600;}

/**
* 使用过滤器配置初始化本过滤器实例成员变量(如果需要则重写)
* @param properties 配置
*/
default public void filterConfig(Properties properties){}

/**
* Token通过哪个字段携带到Emqx服务端
* @return 携带字段
*/
default public String getTokenfrom(){return "password";}

/**
* 使用Flume插件配置初始化本过滤器实例成员变量(如果需要则重写)
* @param config 配置
*/
default public void pluginConfig(Map<String,String> config){}
```

除非有特别的必要，否则程序员编写过滤器无需实现pluginConfig接口，该接口回调传入的字典参数来自于插件上下文配置（即flume进程启动时由-f参数显式指定的配置文件），而对于filterConfig接口的实现对于开发工程师而言也是可选的，为了尽量减轻开发工程师编码的复杂性，EmqxSource插件会在初始化插件上下文参数后自动为开发工程师定义的过滤器类完成一次基于过滤器成员变量的依赖注入，以保证在插件在回调过滤器的doFilter方法之前已经充分准备好了所需的过滤器参数，当然开发工程师也可以手动重写此方法以覆盖插件的初始化结果
            
    
**  备注： **    
EmqxSource插件启动时会自动将Flume安装目录下的filter子目录递归装载到JVM的CLASSPATH路径下，因此在filter目录下的任何子目录都将存在于类路径的根目录下，所以，运维工程师或开发工程师可以随时将过滤器的配置文件、字节码文件或打包好的JAR文件等放入filter目录下的任何位置均可，EmqxSource插件总是可以准确无误的找到并读取他们；这一点是非常重要的，它保证了放入此目录下的任何文件都将存在于CLASSPATH路径上，程序员自定义的过滤器可以毫无障碍的找到并实现过滤器的上下文参数配置；为了方便在配置和代码多了之后，其后期维护难度不至于过大，我们建议开发工程师和运维工程师应该在此目录下建立起更易于方便阅读的目录结构，然后再将过滤器的配置文件、过滤器字节码或过滤器打包JAR文件放置到相应的目录下，一个典型的目录结构设计形如下面的形式：    
      

```Shell
[root@CC7 filter]# pwd
/software/flume-1.9.0/filter
[root@CC7 filter]# tree
.
├── conf
│   └── LogFilter.properties
└── lib
    └── LoggerFilter.class

2 directories, 2 files
```


​    
##### EmqxSource安全认证  
如果你的存储介质中保存的是与用户信息无关的脱敏数据，同时存储服务部署于内网，则不建议使用安全认证，因为安全认证本身将给内网通信带来更多的附加网络阻力，如果在特定的场景下需要Emqx做安全认证，则可以在Emqx服务中开启安全认证，这需要首先在MongoDB中配置数据库用户：  
```Shell
[root@CC9 sbin]# pwd
/software/mongodb-4.4.1/sbin
./SingleTools addAdmin -au root -ap 123456
./SingleTools addUser -d mdbtest -au ligang -ap 123456
```
addAdmin是为admin数据库增加一个超级用户root，第二个addUser是为指定的mdbtest数据库增加用户ligang，超级用户的角色为root，拥有所有读写和执行权限，普通用户ligang只有读写权限，其中-d参数执行增加用户所在的数据库，-au指定新增用户的登录用户名，-ap参数指定新增用户的登录密码，最后启动MongoDB服务时需要带上-a参数（--auth）表示任何形式的客户端登录到MongoDB数据库都需要验证：   
```Shell
[root@CC6 etc]# pwd
/software/emqx4.2.1/etc
[root@CC6 etc]# vi emqx.conf +582
allow_anonymous = false

[root@CC6 etc]# vi plugins/emqx_auth_jwt.conf
auth.jwt.secret=emqxsecret
auth.jwt.from=password

[root@CC6 bin]# pwd
/software/emqx4.2.1/bin
[root@CC6 bin]# ./emqx start
EMQ X Broker 4.2.1 is started successfully!

[root@CC6 bin]# ./emqx_ctl plugins load emqx_auth_jwt
Plugin emqx_auth_jwt loaded successfully.
```

EmqxSource插件也支持Emqx的安全认证，这需要通过配置和过滤器来实现，具体操作步骤详情如下：  
1. 在Flume插件配置文件或过滤器配置文件中增加登录认证信息  
* 在插件配置文件中增加  
```Text
a1.sources.s1.jwtSecret=emqxsecret
a1.sources.s1.tokenFrom=password
a1.sources.s1.tokenExpire=-1
```
* 在过滤器配置文件中增加  
```Text
jwtSecret=emqxsecret
tokenFrom=password
tokenExpire=-1 
```

2. 在自定义过滤器中覆盖以下方法并返回用户名和密码  
```
import com.github.lixiang2114.flume.plugin.emqx.filter.EmqxSourceFilter;
/**
 * @author Louis(LiXiang)
 * @description 自定义日志过滤器
 */
public class EmqxLoggerFilter implements EmqxSourceFilter{
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
}
```