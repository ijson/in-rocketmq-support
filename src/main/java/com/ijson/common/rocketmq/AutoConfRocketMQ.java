package com.ijson.common.rocketmq;

import com.google.common.collect.Maps;
import com.ijson.config.ConfigFactory;
import com.ijson.config.api.IConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Map;


/**
 * RocketMQ 配置中心配置
 * Created by cuiyongxu on 16/1/4.
 */
@Slf4j
public abstract class AutoConfRocketMQ {


    public static final String KEY_NAME_SERVER = "NAMESERVER";
    public static final String KEY_TOPICS = "TOPICS";

    //配置项目在配置中心的名字
    protected volatile String configName;
    //name server 在配置中心的key值
    protected volatile String nameServerKey;
    //group name 在配置中心的key值
    protected volatile String groupKey;
    //topics 在配置中心的key值
    protected volatile String topicKey;

    protected volatile String nameServer;

    protected volatile String groupName;

    protected volatile int maxMessageSize;

    protected volatile int consumeThreadMin = 20;

    protected volatile int consumeThreadMax = 64;

    protected volatile int fetchSize = 10;

    protected volatile int consumeMessageBatchMaxSize = 1;

    protected volatile long pullInterval = 0;

    protected volatile Map<String, String> topics = Maps.newConcurrentMap();

    public AutoConfRocketMQ(String configName, String nameServerKey, String groupKey, String topicKey) {

        if (StringUtils.isBlank(configName) || StringUtils.isBlank(nameServerKey) ||
                StringUtils.isBlank(groupKey)) {
            throw new IllegalArgumentException(String.format("configName=%s,nameServerKey=%s,groupKey=%s,topicKey=%s",
                    configName, nameServerKey, groupKey, topicKey));
        }

        this.configName = configName;
        this.nameServerKey = nameServerKey;
        this.groupKey = groupKey;
        this.topicKey = topicKey;
    }

    public AutoConfRocketMQ(String nameServer, String groupName, String topicName, int consumeThreadMin, int consumeThreadMax) {
        if (StringUtils.isBlank(nameServer) || StringUtils.isBlank(groupName)) {
            throw new IllegalArgumentException(String.format("nameServer=%s,groupName=%s", nameServer, groupName));
        }
        this.nameServer = nameServer;
        log.info("nameServer:" + nameServer);
        this.groupName = groupName;
        log.info("groupName:" + groupName);
        if (consumeThreadMin > 0) {
            this.consumeThreadMin = consumeThreadMin;
            log.info("consumeThreadMin:" + this.consumeThreadMin);
        }
        if (consumeThreadMax > 0) {
            this.consumeThreadMax = consumeThreadMax;
            log.info("consumeThreadMax:" + this.consumeThreadMax);
        }
        this.maxMessageSize = 1024 * 1024;
        log.info("maxMessageSize:" + maxMessageSize + "M");
        this.topics = parseTopics(topicName);
        log.info("topicKey:" + topicName);
    }


    public void init() {
        ConfigFactory.getConfig(configName, this::reload);
    }

    protected abstract void doReload();

    protected abstract void shutDown();

    protected void reload(IConfig config) {
        log.info("====== start load rocketMQ " + configName + " config ======");
        this.nameServer = config.get(nameServerKey);
        log.info("nameServer:" + nameServer);
        this.groupName = config.get(groupKey);
        log.info("groupName:" + groupName);
        this.consumeThreadMin = config.getInt("CONSUME_THREAD_MIN", 20);
        log.info("consumeThreadMin:" + consumeThreadMin);
        this.consumeThreadMax = config.getInt("CONSUME_THREAD_MAX", 64);
        log.info("consumeThreadMax:" + consumeThreadMax);
        this.maxMessageSize = config.getInt("MAXMESSAGESIZE", 1) * 1024 * 1024;
        log.info("maxMessageSize:" + maxMessageSize + "M");
        this.fetchSize = config.getInt("FETCHSIZE", 10);
        log.info("fetchSize:" + fetchSize);
        this.consumeMessageBatchMaxSize = config.getInt("CONSUMEMESSAGEBATCHMAXSIZE", 1);
        log.info("consumeMessageBatchMaxSize:" + consumeMessageBatchMaxSize);
        this.pullInterval = config.getInt("PULLINTERVAL", 0);
        log.info("pullInterval:" + pullInterval);
        if (topicKey != null) {
            this.topics = parseTopics(config.get(topicKey));
        }
        log.info("topicKey:" + topics);
        log.info("====== start load rocketMQ " + configName + " config ======");
        doReload();
    }

    private Map<String, String> parseTopics(String topicsStr) {

        if (StringUtils.isBlank(topicsStr)) {
            return Collections.emptyMap();
        }
        String[] topicArray = StringUtils.split(topicsStr, ",");

        Map<String, String> ret = Maps.newHashMap();
        for (String topic : topicArray) {
            String[] entry = StringUtils.split(topic, ":");
            if (entry.length == 0) {
                continue;
            } else if (entry.length == 1) {
                ret.put(entry[0], "*");
            } else {
                ret.put(entry[0], entry[1]);
            }
        }
        return ret;
    }

    public String getNameServer() {
        return nameServer;
    }

    public String getGroupName() {
        return groupName;
    }

    public Map<String, String> getTopics() {
        return Collections.unmodifiableMap(topics);
    }

    public String getTopic() {

        if (topics != null) {
            return topics.keySet().iterator().next();
        }

        return null;
    }
}
