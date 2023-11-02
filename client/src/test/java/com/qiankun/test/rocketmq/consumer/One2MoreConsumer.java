package com.qiankun.test.rocketmq.consumer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * 单生产者多消费者 One2More 消费者
 */
@Slf4j
public class One2MoreConsumer {
    public static void main(String[] args) throws MQClientException {
        // 1.创建消费者对象
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("one2moreGroup");
        consumer.setConsumeTimeout(30000);
        // 2.设置NameServer
        consumer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        // 设置负载均衡策略
        consumer.setMessageModel(MessageModel.BROADCASTING);
        // 3. 订阅 Topic tag
        consumer.subscribe("one2moreTopic","*");
        // 4.添加监听
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    log.info("接收到的消息为：{}",messageExt);
                    log.info("消息内容：{}", new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5.开启消费者服务
        consumer.start();
    }
}
