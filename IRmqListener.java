package com.etrans.lib.rocket;

import com.alibaba.rocketmq.common.message.MessageExt;

/**
 * Created by maotz on 2015-05-09.
 * 监听器接口
 */
public interface IRmqListener {
    /**
     * 处理接收到的消息
     * @param _msg 来自MQ的消息
     */
    public void onRmqMsg(MessageExt _msg);
}
