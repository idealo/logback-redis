#Logback Redis Batch Appender 
 [![Build Status](https://travis-ci.org/idealo/logback-redis.svg?branch=master)](https://travis-ci.org/idealo/logback-redis)
 [![Maven Central](https://maven-badges.herokuapp.com/maven-central/de.idealo.logback/logback-redis/badge.svg)](https://maven-badges.herokuapp.com/maven-central/de.idealo.logback/logback-redis) 

Enables Java applications to log directly to [redis](http://redis.io/) via the [jedis](https://github.com/xetorthio/jedis) client as part of centralized logging with the [ELK](https://www.elastic.co/products) stack.

More specifically, it uses [async appenders](https://github.com/logstash/logstash-logback-encoder#async) and [JSON encoding](https://github.com/logstash/logstash-logback-encoder#composite_encoder) of the [logstash-logback-encoder](https://github.com/logstash/logstash-logback-encoder) project. Messages are sent to redis in [batches](http://redis.io/topics/pipelining) for performance reasons. The [redis sentinel functionality](http://redis.io/topics/sentinel) is supported.

## Maven dependencies
``` xml
<dependency>
   <groupId>de.idealo.logback</groupId>
   <artifactId>logback-redis</artifactId>
   <version>0.9.3</version>
</dependency>
```

used best in conjuction with

```xml
<dependency>
   <groupId>net.logstash.logback</groupId>
   <artifactId>logstash-logback-encoder</artifactId>
   <version>4.7</version>
</dependency>      
```

## Configuration
### Simple Configuration 
```xml
<configuration>
    ...
    <appender name="REDIS" class="net.logstash.logback.appender.LoggingEventAsyncDisruptorAppender">
        <appender class="de.idealo.logback.appender.RedisBatchAppender">
            <connectionConfig>
                <scheme>SENTINEL</scheme>
                <sentinelMasterName>${sentinel.master.name}</sentinelMasterName>
                <sentinels>${sentinel.host.list}</sentinels>
                <key>${sentinel.key}</key>
            </connectionConfig>
            <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
                <providers>
                    <arguments/>
                    <mdc/>
                    <pattern>
                        <pattern>
                            {
                            "@stage":"${STAGE}",
                            "app": "${projectName}",
                            "host": "${HOSTNAME}",
                            "key" : "userservices",
                            "level": "%level",
                            "logger": "%logger",
                            "message": "%message",
                            "thread": "%thread",
                            "timestamp": "%d{yyyy-MM-dd'T'HH:mm:ss.SSSZZ}"
                            }
                        </pattern>
                    </pattern>
                    <stackTrace/>
                </providers>
            </encoder>
        </appender>
    </appender>
    ...
</configuration>
```

### Parameters
* connectionConfig:
    * key: key under which messages are stored in redis
    * scheme (NODE | SENTINEL): defines whether redis is accessed via a single node or via [sentinel](http://redis.io/topics/sentinel)
    * for scheme=SENTINEL:
        * sentinelMasterName: name of the sentinel master
        * sentinels: comma separated list of sentinels with the following structure: host1:port1,host2:port2
    * for scheme=NODE:
        * host: redis host
        * port: redis port
* maxBatchMessages: number of messages which are sent as batch size to redis
* maxBatchSeconds: time interval in seconds after a batch of messages is sent to redis if the batch size is not reached 
* encoder: encoder for JSON formatting of the messages
* ringBuffer and waitStrategyType determine [how the logstash-logback-encoder asynchronously processes the messages](https://github.com/logstash/logstash-logback-encoder#async). Note that messages may be lost if the ring buffer size is too small (["If the RingBuffer is full (e.g. due to slow network, etc), then events will be dropped."](https://github.com/logstash/logstash-logback-encoder#async)).  

### Extended Configuration
``` xml
<?xml version="1.0" encoding="UTF-8"?>
<included>
    <shutdownHook class="ch.qos.logback.core.hook.DelayingShutdownHook"/>
    <appender name="REDIS_APPENDER" class="net.logstash.logback.appender.LoggingEventAsyncDisruptorAppender">
        <ringBufferSize>131072</ringBufferSize>
        <appender class="de.idealo.logback.appender.RedisBatchAppender">
            <connectionConfig>
                <!-- redis sentinel: -->
                <scheme>SENTINEL</scheme>
                <sentinelMasterName>mymaster</sentinelMasterName>
                <sentinels>server:26379</sentinels>
                <!-- redis node: -->
                <!--<scheme>NODE</scheme>-->
                <!--<host>server</host>-->
                <!--<port>6379</port>-->
                <key>keyForRedis</key>
            </connectionConfig>
            <maxBatchMessages>1000</maxBatchMessages>
            <maxBatchSeconds>10</maxBatchSeconds>
            <encoder class="net.logstash.logback.encoder.LoggingEventCompositeJsonEncoder">
                <providers>
                    <mdc/>
                    <pattern>
                        <pattern>
                            {
                            "timestamp": "%d{yyyy-MM-dd'T'HH:mm:ss.SSSZZ}",
                            "message": "%message",
                            "logger": "%logger",
                            "thread": "%thread",
                            "level": "%level",
                            "host": "${HOSTNAME}",
                            "file": "%file",
                            "line": "%line",
                            "app": "${projectName}"
                            }
                        </pattern>
                    </pattern>
                    <stackTrace>
                        <throwableConverter class="net.logstash.logback.stacktrace.ShortenedThrowableConverter">
                            <maxDepthPerThrowable>30</maxDepthPerThrowable>
                            <maxLength>4096</maxLength>
                            <shortenedClassNameLength>20</shortenedClassNameLength>
                            <rootCauseFirst>true</rootCauseFirst>
                        </throwableConverter>
                   </stackTrace>
                </providers>
            </encoder>
        </appender>
    </appender>
</included>
```
    
This appender configuration can either be included in a logback.xml file (via "included" tag) or be directly contained in a logback.xml (without "included" tag).

### JSON Format Created by the Appender (Example) (= Input for [Logstash](https://www.elastic.co/products/logstash))
``` js
{
    "_index": "myIndex-2015.09.15.12",
    "_type": "logs",
    "_id": "AU_RAKiIuARjD1TqcEFe",
    "_score": null,
    "_source": {
        "mdcKey1": "value1",
        "mdcKey2": "value2",
        "seq": "198",
        "timestamp": "2015-09-15T14:35:19.256+0200",
        "message": "logback-1:198",
        "logger": "LoggingTest",
        "thread": "main",
        "level": "INFO",
        "host": "myHost",
        "file": "?",
        "line": "?",
        "@version": "1",
        "@timestamp": "2015-09-15T12:35:25.251Z",
        "type": "logs"
},
    "fields": {
    "@timestamp": [
        1442320525251
    ]
},
    "sort": [
    1442320525251
]
}
```

### Logging Appender Errors to a File 
The following logger configuration in the logback.xml is recommended in order to write error messages of the appender directly to a file in error situations (especially if redis is not available):
``` xml
<logger name="de.idealo.logback.appender" level="error" additivity="false">
    <appender-ref ref="FILE_FOR_REDISBATCHAPPENDER"/>
</logger>
```

## Shutdown
### Shutdown Hook
The redis batch appender must be shut down on application shutdown in order to ensure that cleans up background threads and pools and ensures that remaining messages are sent to Redis before shutting down the app. This is performed by the stop method of the redis batch appender that is automatically called when putting a shutdown hook in logback.xml:
``` xml
<shutdownHook class="ch.qos.logback.core.hook.DelayingShutdownHook"/>
```
### [Spring Boot](http://projects.spring.io/spring-boot/) Apps
The shutdown hook above doesn't work in Spring Boot apps when they are shut down via the [actuator](http://docs.spring.io/spring-boot/docs/current-SNAPSHOT/reference/htmlsingle/#production-ready) shutdown URL ([POST {baseUrl}/shutdown](http://docs.spring.io/spring-boot/docs/current/reference/html/production-ready-endpoints.html)). Instead, this can be done by the following Spring component:
``` java
@Component
public class LogbackStopListener implements ApplicationListener<ContextClosedEvent> {
    @Override
    public void onApplicationEvent(final ContextClosedEvent event) {
        LogbackUtils.stopLogback();
    }
}
```
