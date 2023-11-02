package com.qiankun.test.rocketmq.producer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;

/**
 * 延迟消息生产者
 */
@Slf4j
public class DelayProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("delayGroup");
        producer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        producer.start();
        Message msg = new Message("delayTopic","延迟消息".getBytes(StandardCharsets.UTF_8));
        msg.setDelayTimeLevel(3);
        SendResult send = producer.send(msg);
        if (send.getSendStatus().equals(SendStatus.SEND_OK)) {
            while (true){
                Thread.sleep(1000);
                log.info("发送消息成功 ：{}");
            }
        }
        // producer.shutdown();
    }
}
