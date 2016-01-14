
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Created by maotz on 2015-05-09.
 */
public class RmqAdapter implements IRmqListener {
    private final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RmqAdapter.class);

    public void onRmqMsg(MessageExt _msg){
        logger.info("on rmqMsg {}", _msg.getBody() );
    }
}
