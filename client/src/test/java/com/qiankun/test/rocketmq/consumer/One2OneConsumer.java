package com.qiankun.test.rocketmq.consumer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 单生产者对单消费者（One2One) 消费者
 */
@Slf4j
public class One2OneConsumer {
    public static void main(String[] args) throws MQClientException {
        Date date = new Date(1699256119073L);
        // Date date = new Date(1696645813271L);
        // 1696645813271
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(simpleDateFormat.format(date));
        // // 1.创建消费者对象 Consumer
        // DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("one2oneGroup");
        // // 将超时时间设长（默认3000），防止连接超时
        // consumer.setConsumeTimeout(30000);
        // // 2.设定接受的命名服务器
        // consumer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        // // 3.设置接受消息对应的Topic
        // // * 标识对应sub标签为任务  subExpression 为 tags标签
        // consumer.subscribe("one2oneTopic","*");
        // // 4. 开启监听接受消息
        // consumer.registerMessageListener(new MessageListenerConcurrently() {
        //     @Override
        //     public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
        //         for (MessageExt messageExt : list) {
        //             log.info("接收到的消息为：{}",messageExt);
        //             log.info("消息内容：{}",new String(messageExt.getBody()));
        //         }
        //         return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        //     }
        // });
        // // 5.启动服务接受消息
        // consumer.start();
    }
}
