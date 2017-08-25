import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.ijson.common.rocketmq.AutoConfRocketMQProcessor;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by cuiyongxu on 17/8/25.
 */
@Slf4j
public class JIMConsumer {

    public static void main(String[] args) {
        JIMConsumer jimConsumer = new JIMConsumer();
        jimConsumer.executeConsumer();
    }

    public void executeConsumer() {
        String configName = "in-mq-config";
        MessageListenerConcurrently listener = (messages, context) -> {
            messages.forEach(this::processMessage);
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        };
        AutoConfRocketMQProcessor processor = new AutoConfRocketMQProcessor(configName, listener);
        processor.init();
        log.info("loaded " + configName + ", init over");
    }

    private void processMessage(MessageExt message) {
        String msg = new String(message.getBody());
        log.info(">>>>>>>>>>{},{}", message.getMsgId(), msg);
    }
}
