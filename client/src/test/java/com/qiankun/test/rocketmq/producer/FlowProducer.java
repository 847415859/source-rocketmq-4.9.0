package com.qiankun.test.rocketmq.producer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * @Description:
 * @Date : 2023/02/24 11:00
 * @Auther : tiankun
 */
@Slf4j
public class FlowProducer {

    private static final String GROUP_NAME = "flowGroup";

    private static final String TOPIC_NAME = "flow";

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer(GROUP_NAME);
        producer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        producer.start();
        for (int i = 0; i < 100; i++) {
            SendResult send = producer.send(new Message(TOPIC_NAME, ("消息" + i).getBytes(StandardCharsets.UTF_8)));
            log.info("发送消息成功：{}",send);
        }
    }

}
