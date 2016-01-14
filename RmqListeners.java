
import com.alibaba.rocketmq.common.message.MessageExt;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by maotz on 2015-05-09.
 *
 */
public class RmqListeners implements IRmqListener{

    private final List<IRmqListener> list = new LinkedList<IRmqListener>();

    public RmqListeners(){

    }

    public void onRmqMsg(MessageExt _msg){
        for(IRmqListener listener:list)
            listener.onRmqMsg(_msg);
    }

    public void registerListener(IRmqListener _listener){
        if(null!=_listener)
            if(!list.contains(_listener))
                list.add(_listener);
    }
}
