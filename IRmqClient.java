
import com.alibaba.rocketmq.client.producer.SendResult;

/**
 * Created by maotz on 2015-05-09.
 * 访问 RocketMQ 的客户端接口
 */
public interface IRmqClient {
    /**
     * 初始化
     * @param _group 分组
     */
    public void setGroup(String _group);

    /**
     * 设置实例名
     * @param _instance 实例名
     */
    public void setInstance(String _instance);

    /**
     *
     * @param _mqNameServers 服务器地址
     */
    public void setup(String _mqNameServers);

    /**
     * 添加监听器
     * @param _listener 监听器
     */
    public void addListener(IRmqListener _listener);

    /**
     * 启动，关闭
     * @param _active 开关
     */
    public void setActive(boolean _active) throws Exception;

    /**
     * 订阅
     * @param _topic 主题
     * @param subExpression 条件
     */
    public void subscribe(String _topic, String subExpression) throws Exception;

    public SendResult send(String topic, String tags, String keys, int flag, byte[] body) throws Exception;
}
