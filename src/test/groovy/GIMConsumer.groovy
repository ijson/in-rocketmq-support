import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently
import com.alibaba.rocketmq.common.message.MessageExt
import com.ijson.common.rocketmq.AutoConfRocketMQProcessor
import spock.lang.Specification

/**
 * Created by cuiyongxu on 17/8/25.
 */
class GIMConsumer extends Specification {
    def configName = "in-mq-config"

    def "接收MQ消息"() {

        given:
        MessageListenerConcurrently listener = new MessageListenerConcurrentlyImpl();
        AutoConfRocketMQProcessor processor = new AutoConfRocketMQProcessor(configName, listener);
        processor.init();
    }

    class MessageListenerConcurrentlyImpl implements MessageListenerConcurrently {
        @Override
        ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> messages, ConsumeConcurrentlyContext context) {
            messages.forEach({
                message ->
                    message;
                    getProcessMessage(message)
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }

        def getProcessMessage(MessageExt message) {

            String msg = new String(message.getBody());
            println msg
            println ">>>>>>>>>>>>>>>>>" + message.msgId
        }
    }
}
