
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.message.Message;

/**
 * Created by maotz on 2015-05-21.
 * 消息生产者
 */
public class RmqProducer {
    private final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RmqProducer.class);
    private final DefaultMQProducer producer = new DefaultMQProducer();

    /**
     * 构造函数
     */
    public RmqProducer(){
        producer.setRetryAnotherBrokerWhenNotStoreOK(true);
    }

    /**
     * 设置参数
     * @param _addrs 服务器地址
     * @param _instance 实例名称
     * @param _group 分组名称
     */
    public void setup(String _addrs, String _instance, String _group){
        if(null == _addrs || _addrs.isEmpty()){
            logger.error("RmqProducer setup _addrs is null.");
            return;
        }

        if(null == _instance && _instance.isEmpty()){
            logger.error("RmqProducer setup _instance is null.");
            return;
        }

        if(null == _group && _group.isEmpty()){
            logger.error("RmqProducer setup _group is null.");
            return;
        }

        logger.info("setup({},{},{})", _addrs, _instance, _group);
        producer.setProducerGroup(_group);
        producer.setInstanceName(_instance);
        producer.setNamesrvAddr(_addrs);
    }

    /**
     * 启动
     */
    public void start() throws Exception{
        String addrs = producer.getNamesrvAddr();
        if(null == addrs || addrs.isEmpty())
            throw new Exception("NamesvrAddr is empty");
        
        logger.debug("start.{}.enter", addrs);
        producer.start();
        logger.debug("start.{}.leave", addrs);
    }

    /**
     * 新建主题
     * @param _topicName 主题名称
     * @throws Exception 异常
     */
    public void createTopic(String _topicName) throws Exception{
        producer.createTopic("", _topicName, TopicConfig.DefaultReadQueueNums);
    }

    /**
     * 推送数据
     * @param _topic 主题
     * @param _tags 消息标签，只支持设置一个Tag（服务端消息过滤使用）
     * @param _key  消息关键词，多个Key用KEY_SEPARATOR隔开（查询消息使用）
     * @param _flag 消息标志，系统不做干预，完全由应用决定如何使用
     * @param _data 消息体
     * @throws Exception 异常
     */
    public SendResult push(String _topic, String _tags, String _key, int _flag, byte[] _data) throws Exception{
        Message msg = new Message(_topic, _tags, _key, _flag, _data, false);
        return producer.send(msg);
    }

    /**
     * 推送数据
     * @param _topic 主题
     * @param _flag 消息标志，系统不做干预，完全由应用决定如何使用
     * @param _data 消息体
     * @throws Exception 异常
     */
    public SendResult push(String _topic, int _flag, byte[] _data) throws Exception{
    	return push(_topic, "", "", _flag, _data);
    }

    /**
     * 推送数据
     * @param _topic 主题
     * @param _body 消息体
     * @throws Exception 异常
     */
    public SendResult push(String _topic, String _body) throws Exception{
    	return push(_topic, 0, _body.getBytes());
    }

    /**
     * 样例
     * @param arg
     * @throws Exception 异常
     */
    public static void main(String[] arg) throws Exception{
        RmqProducer producer = new RmqProducer();
        producer.setup("10.10.1.42:9876", "mousPI", "mousPG");
        producer.start();
        
//        producer.createTopic("RAW_D_74");
        
        int i=0;
        while(true){
            logger.debug(":::::::: push msg-{} ", i++);
            SendResult sendResult = producer.push("RAW_D_74", "msg "+i);
            logger.info("{}", sendResult);
            
            Thread.sleep(3000);
        }
    }
}
