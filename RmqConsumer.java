
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by maotz on 2015-05-21.
 * 封装RMQ消费者
 */
public class RmqConsumer {
    private final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RmqConsumer.class);

    private final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
    private final RmqListeners listeners = new RmqListeners();
    private final BlockingQueue<MessageExt> msgQueue = new LinkedBlockingQueue<>(1024);
    private volatile boolean active;

    public RmqConsumer() {
        setConsumeFromNowTime(true);
        consumer.setConsumeThreadMin(1);
        consumer.setConsumeThreadMax(1);
        //consumer.setOffsetStore(null);
        //consumer.setOffsetStore(new RemoteBrokerOffsetStore());
        //consumer.setHeartbeatBrokerInterval(5000);// 断开监测时长，毫秒
        //consumer.setPollNameServerInteval(1000*60); // 名字查询间隔，毫秒
        consumer.setMessageModel(MessageModel.BROADCASTING);
        
        consumer.registerMessageListener(new MessageListenerOrderly() {
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
                for (MessageExt msg : msgs)
                    if (msg.getBody().length > 0)
                        try {
                            msgQueue.put(msg);
                        }catch (InterruptedException e){
                            logger.error("error on put msg ", e);
                        }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
    }

    private final Runnable generator = new Runnable() {
        @Override
        public void run() {
            while(active) {
                try {
                    MessageExt msg = msgQueue.take();
                    listeners.onRmqMsg(msg);
                }catch (Exception e){
                    logger.error("error on msg ", e);
                }
            }
        }
    };

    /**
     * 设置初始化参数
     * @param _addrs 服务器地址
     * @param _instance 实例名称
     * @param _group 分组
     */
    public void setup(String _addrs, String _instance, String _group){
        logger.info("setup({},{},{})", _addrs, _instance, _group);
        consumer.setNamesrvAddr(_addrs);
        consumer.setInstanceName(_instance);
        consumer.setConsumerGroup(_group);
    }

    /**
     * 设置消费开始处
     * @param _from_now: true : 从当前时间开,false：从上次结束点继续
     */
    public void setConsumeFromNowTime(boolean _from_now){
        if(_from_now) {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
            consumer.setConsumeTimestamp(UtilAll.timeMillisToHumanString3(System.currentTimeMillis()));
        }else {
            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        }
    }

    /**
     * 注册数据监听器
     * @param _listener 监听器
     */
    public void registerListener(IRmqListener _listener){
        listeners.registerListener(_listener);
    }

    /**
     * 订阅
     * @param _topic 主题
     * @param _exp 表达式
     * @throws Exception
     */
    public void subscribe(String _topic, String _exp) throws Exception{
        //logger.debug("subscribe({},{})", _topic, _exp);
        consumer.subscribe(_topic, _exp);
    }

    /**
     * 启动
     * @throws Exception 异常
     */
    public void start() throws Exception{
        String addrs = consumer.getNamesrvAddr();
        if(null==addrs || addrs.isEmpty())
            throw new Exception("NamesvrAddr is empty");
        active = true;
        Executors.newSingleThreadExecutor().execute(generator);
//        logger.info("start.{}.enter", addrs);
        consumer.start();
//        logger.info("start.{}.leave", addrs);
    }

    public static void main(String[] args) throws Exception{
        RmqConsumer consumer = new RmqConsumer();
        consumer.setup("192.168.4.22:9876", "mousI", "mousG");
        consumer.subscribe("RAW_D_74", "");
        consumer.start();
    }
}
