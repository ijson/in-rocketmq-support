package com.ijson.common.rocketmq;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 * 支持制动化配置的RocketMQ消息处理器
 * Created by cuiyongxu on 16/1/4.
 */
public class AutoConfRocketMQProcessor extends AutoConfRocketMQ {

    private static final Logger LOG = LoggerFactory.getLogger(AutoConfRocketMQProcessor.class);

    public static final String KEY_GROUP_CONSUMER = "GROUP_CONSUMER";


    private volatile DefaultMQPushConsumer consumer;

    private final MessageListener listener;

    public AutoConfRocketMQProcessor(String configName, String nameServerKey, String groupKey, String topicKey,
                                     MessageListener listener) {
        super(configName, nameServerKey, groupKey, topicKey);
        this.listener = listener;
    }


    public AutoConfRocketMQProcessor(String nameServerKey, String groupKey, String topicKey, int consumeThreadMin,int consumeThreadMax, MessageListener listener) {
        super(nameServerKey, groupKey, topicKey,consumeThreadMin,consumeThreadMax);
        this.listener = listener;
    }


    public AutoConfRocketMQProcessor(String configName, MessageListener listener) {
        this(configName, KEY_NAME_SERVER, KEY_GROUP_CONSUMER, KEY_TOPICS, listener);
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    @Override
    public void doReload() {

        if (consumer == null) {
            LOG.debug("CreateRocketMQConsumer");
            createConsumer();
        } else {
            LOG.debug("ReCreateRocketMQConsumer");
            shutDownConsumer();
            createConsumer();
        }
    }

    private void createConsumer() {
        consumer = new DefaultMQPushConsumer(getGroupName());
        consumer.setNamesrvAddr(nameServer);
        subscribeTopics();
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setPullBatchSize(fetchSize);
        consumer.setConsumeThreadMin(consumeThreadMin);
        consumer.setConsumeThreadMax(consumeThreadMax);
        consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);
        consumer.setPullInterval(pullInterval);
        if (listener instanceof MessageListenerConcurrently) {
            consumer.registerMessageListener((MessageListenerConcurrently) this.listener);
        } else if (listener instanceof MessageListenerOrderly) {
            consumer.registerMessageListener((MessageListenerOrderly) this.listener);
        } else {
            throw new RuntimeException("UnSupportListenerType");
        }

        try {
            consumer.start();
        } catch (MQClientException e) {
            LOG.error("StartRocketMQConsumer Error", e);
        }
        LOG.info("RocketMQConsumer Started! group={} instance={}", consumer.getConsumerGroup(),
                 consumer.getInstanceName());
    }

    private void subscribeTopics() {
        subscribeTopics(consumer, topics);
    }

    protected void subscribeTopics(DefaultMQPushConsumer consumer, Map<String, String> topics) {
        try {
            for (Map.Entry<String, String> i : topics.entrySet()) {
                consumer.subscribe(i.getKey(), i.getValue());
            }
        } catch (MQClientException e) {
            LOG.error("SubscribeTopic Error!", e);
        }
    }

    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    private void shutDownConsumer() {
        if (this.consumer != null) {
            try {
                this.consumer.shutdown();
                this.consumer = null;
            } catch (Exception e) {
                LOG.error("ShutRocketMQConsumer Error,nameServer={} group={}", consumer.getNamesrvAddr(),
                          consumer.getConsumerGroup(), e);
            }
        }
    }

    @Override
    public void shutDown() {
        shutDownConsumer();
    }

}
