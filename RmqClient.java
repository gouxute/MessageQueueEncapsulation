
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Created by maotz on 2015-05-09.
 *  消息队列客户端
 */
public class RmqClient implements IRmqClient {
    private final static Logger logger = LoggerFactory.getLogger(RmqClient.class);

    private DefaultMQProducer mqProducer = null;
    private DefaultMQPushConsumer mqConsumer = null;

    private String mqGroupName = "group";
    private String mqInstanceName = "instance";
    private String mqNameServers;
    private boolean active;
    private final boolean enableRecv;
    private final boolean enableSend;
    private final RmqListeners listeners = new RmqListeners();

    public RmqClient(boolean _enable_recv, boolean _enable_send){
        enableRecv = _enable_recv;
        enableSend = _enable_send;
    }

    private final MessageListener mqListener = new MessageListenerConcurrently() {

		public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

			for(MessageExt msg : msgs)
				onReceive(msg);

			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}
	};
    
	protected void onReceive(MessageExt _msg){
		listeners.onRmqMsg(_msg);
	}
	
    /**
     * 生成消费者
     * @throws Exception
     */
    private void makeConsumer() throws Exception{
        if(null == mqConsumer){
            mqConsumer = new DefaultMQPushConsumer(mqGroupName);
            mqConsumer.setNamesrvAddr( mqNameServers );
            mqConsumer.setInstanceName(mqInstanceName);
            mqConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
            mqConsumer.registerMessageListener(mqListener);
            mqConsumer.start();
            logger.info("Consumer-> GroupName:{} InstanceName:{} <{}> is ok.", mqGroupName, mqInstanceName, mqNameServers);
        }
    }

    /**
     * 生成生产者
     * @throws Exception
     */
    private void makeProducer() throws Exception{
        if(null == mqProducer){
            mqProducer = new DefaultMQProducer(mqGroupName);
            mqProducer.setNamesrvAddr(mqNameServers);
            mqProducer.setInstanceName(mqInstanceName);
            mqProducer.setProducerGroup(mqGroupName);
            mqProducer.setRetryAnotherBrokerWhenNotStoreOK(true);
            mqProducer.start();
            logger.info("Producer-> GroupName:{} InstanceName:{} <{}> is ok.", mqGroupName, mqInstanceName, mqNameServers);
        }
    }

    /**
     * 初始化
     * @param _group 分组
     */
    public void setGroup(String _group){
        mqGroupName = _group;
    }

    /**
     * 设置实例名
     * @param _instance 实例名
     */
    public void setInstance(String _instance){
        mqInstanceName = _instance;
    }

    /**
     *
     * @param _mqNameServers 服务器地址
     */
    public void setup(String _mqNameServers){
        mqNameServers = _mqNameServers;
    }

    /**
     * 添加监听器
     * @param _listener 监听器
     */
    public void addListener(IRmqListener _listener){
        listeners.registerListener(_listener);
    }

    /**
     * 开启链接，准备工作
     */
    private void start() throws Exception{
        if(enableSend)
            makeProducer();
        
        if(enableRecv)
            makeConsumer();
    }

    /**
     * 停止工作
     */
    private void stop(){
        if(enableSend)
            mqConsumer.shutdown();
        if(enableRecv)
            mqProducer.shutdown();
    }

    /**
     * 启动，关闭
     * @param _active 开关
     */
    public void setActive(boolean _active) throws Exception{
        if(active!=_active) {
            active = _active;

            if(active)
                start();
            else
                stop();
        }
    }

    public void subscribe(String _topic) throws Exception{
        subscribe(_topic, "*");
    }
    /**
     * 订阅
     * @param _topic 主题
     * @param _subExpression 条件
     */
    public void subscribe(String _topic, String _subExpression) throws Exception{
        if(active & enableRecv){
            if(null==_subExpression || _subExpression.length()==0)
                _subExpression = "*";
            mqConsumer.subscribe(_topic, _subExpression);
        }
    }

    /**
     * 发送数据
     * @param _topic 主题
     * @param _body  内容
     * @throws Exception
     */
    public SendResult send(String _topic, String _body) throws Exception{
       return send( _topic, "", "", 0, _body.getBytes() );
    }

    /**
     * 发送数据
     * @param _topic
     * @param _flag
     * @param _body
     * @return
     * @throws Exception
     */
    public SendResult send(String _topic, int _flag, byte[] _body) throws Exception{
    	return send( _topic, "", "", _flag, _body);
    }

    /**
     * 发送数据
     * @param topic 主题
     * @param tags 标记
     * @param keys 关键字
     * @param flag flag
     * @param body 内容
     * @throws Exception
     */
    public SendResult send(String topic, String tags, String keys, int flag, byte[] body) throws Exception{
        if(active & enableSend){
            Message msg = new Message(topic, tags, keys, flag, body, true);
            return mqProducer.send(msg);
        }        
        return null;
    }
    
    /**
     * 样例
     * @param args
     * @throws Exception
     */
	public static void main(String[] args) throws Exception {
		RmqClient mqClient = new RmqClient(false, true);
		mqClient.setGroup("ConsumerApp");
		mqClient.setInstance("Consumer");
        mqClient.setup("10.10.1.42:9876");
        mqClient.addListener(new RmqAdapter());
        mqClient.setActive(true);
	}	
}
