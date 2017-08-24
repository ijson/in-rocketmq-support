package com.ijson.common.rocketmq;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.client.producer.SendStatus;
import com.alibaba.rocketmq.common.message.Message;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * 支持自动化配置的RocketMQ同步消息发送者
 * Created by cuiyongxu on 16/1/4.
 */
@Slf4j
public class AutoConfRocketMQSender extends AutoConfRocketMQ {


    public static final String KEY_GROUP_PROVIDER = "GROUP_PROVIDER";


    private volatile DefaultMQProducer sender;

    public AutoConfRocketMQSender(String configName, String nameServerKey, String groupKey, String topicKey) {
        super(configName, nameServerKey, groupKey, topicKey);
    }

    public AutoConfRocketMQSender(String nameServerKey, String groupKey, String topicKey) {
        super(nameServerKey, groupKey, topicKey,0,0);
    }



    public AutoConfRocketMQSender(String configName) {
        this(configName, KEY_NAME_SERVER, KEY_GROUP_PROVIDER, KEY_TOPICS);
    }

    public DefaultMQProducer getSender() {
        return sender;
    }

    /**
     * 配置发生变化时加载
     * sender volatile 并不保证线程安全，有丢消息风险。
     * 尽量不要依赖动态变更
     */
    @Override
    public void doReload() {

        if (sender == null) {
            createProducer();
        } else {
            shutDownProducer();
            createProducer();
        }
    }

    /**
     * 同步消息发送
     */
    public void send(Message message) {

        if (message.getTopic() == null && getTopic() != null) {
            message.setTopic(getTopic());
            doSend(message);
        } else if(message.getTopic() != null ) {
            doSend(message);
        } else {
            log.warn("DiscardNoTopicMessage");
        }
    }

    private boolean isValidTopic(String topic) {
        if (this.topics == null || topics.isEmpty()) {
            return false;
        }
        return topics.containsKey(topic);
    }

    private void doSend(Message msg) {
        try {
            SendResult result = sender.send(msg);
            SendStatus status = result.getSendStatus();
            if (status.equals(SendStatus.SEND_OK)) {
                log.debug("msgId={}, status={}", result.getMsgId(), status);
            } else {
                log.error("msgId={}, status={}", result.getMsgId(), status);
            }
        } catch (Exception e) {
            log.error("SendError,message={}", msg, e);
        }
    }

    private void createProducer() {
        try {
            sender = new DefaultMQProducer(groupName);
            sender.setNamesrvAddr(nameServer);
            sender.setMaxMessageSize(maxMessageSize);
            sender.setInstanceName(UUID.randomUUID().toString());
            sender.start();
        } catch (MQClientException e) {
            log.error("CanNotCreateProducer nameServer={} group={} ", nameServer, groupName, e);
        }
    }

    private void shutDownProducer() {
        if (this.sender != null) {
            try {
                this.sender.shutdown();
                this.sender = null;
            } catch (Exception e) {
                log.error("ShutRocketMQDownProducer Error,nameServer={} group={}", sender.getNamesrvAddr(),
                          sender.getProducerGroup(), e);
            }
        }
    }

    @Override
    public void shutDown() {
        shutDownProducer();
    }


}
