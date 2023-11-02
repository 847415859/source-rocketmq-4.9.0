package com.qiankun.test.rocketmq.producer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * 过滤消息 生产者
 * 1. tag
 * 2. SQL
 */
@Slf4j
public class FilterProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("filterGroup");
        producer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        producer.start();

        Message msg = new Message("filterTopic","过滤消息".getBytes(StandardCharsets.UTF_8));
        msg.putUserProperty("age","23");
        producer.send(msg);
        producer.shutdown();
    }
}
