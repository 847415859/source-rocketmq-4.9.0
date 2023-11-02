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
 * 消息类型
 *  同步消息
 *  异步消息
 *  单向消息
 */
@Slf4j
public class MessageTypeProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("messageTypeGroup");
        producer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        producer.start();
        // 同步消息
        // producer.send(new Message("messageTypeTopic", "同步消息".getBytes(StandardCharsets.UTF_8)));
        // 异步消息
        // producer.send(new Message("messageTypeTopic", "异步消息".getBytes(StandardCharsets.UTF_8)), new SendCallback() {
        //     @Override
        //     public void onSuccess(SendResult sendResult) {
        //         log.info("消息发送成功：{}",sendResult);
        //     }
        //
        //     @Override
        //     public void onException(Throwable throwable) {
        //         log.warn("消息发送失败：{}",throwable);
        //     }
        // });
        // 单向消息
        producer.sendOneway(new Message("messageTypeTopic", "单向消息".getBytes(StandardCharsets.UTF_8)));
    }
}
