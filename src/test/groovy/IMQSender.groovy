import com.alibaba.rocketmq.common.message.Message
import com.ijson.common.rocketmq.AutoConfRocketMQSender
import com.ijson.rest.proxy.util.JsonUtil
import lombok.Data
import spock.lang.Specification

import java.text.SimpleDateFormat

/**
 * Created by cuiyongxu on 17/8/25.
 */
class IMQSender extends Specification {

    def configName = "in-mq-config"

    def "发送MQ消息"() {
        given:
        AutoConfRocketMQSender sender = new AutoConfRocketMQSender(configName)
        sender.init()
        def message = new Message()
        def user = new User();
        user.userId = "123456";
        user.userName = "中国"
        user.date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss sss").format(new Date())
        message.body = JsonUtil.toJson(user).getBytes();
        sender.send(message)
    }

    @Data
    class User {
        private String userId;
        private String userName;
        private String date;
    }
}
