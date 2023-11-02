package com.qiankun.test.rocketmq.consumer;

import com.qiankun.test.rocketmq.Default;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class QueryingMessageDemo {
    public static void main(String[] args) throws InterruptedException,
            RemotingException, MQClientException, MQBrokerException {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("consumer_grp_09_01");
        consumer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        consumer.start();
        // 参数一：topic
        // 参数2：msgId
        String msgId = "7F00000185242437C6DC997E7E420000";
        MessageExt message = consumer.viewMessage("one2oneTopic",msgId);
        System.out.println(message);
        System.out.println(message.getMsgId());
        consumer.shutdown();
    }
}