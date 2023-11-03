package com.qiankun.test.rocketmq.producer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * 单生产者对单消费者(One2One)       生产者
 */
@Slf4j
public class One2OneProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException, IOException {
        // 1.创建一个发消息的Producer 并指定组名
        DefaultMQProducer producer = new DefaultMQProducer("one2oneGroup");
        // 把超时时间设置的长一点 防止出现以下错误 默认超时间是3000s
        // org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException: sendDefaultImpl call timeout
        producer.setSendMsgTimeout(30000);
        // 2.设置发送的命名服务器（NameServer）地址
        producer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        // 3.启动生产者服务
        producer.start();
        // 4.发送消息
        // Message message = new Message("one2oneTopic","one2oneTags","Hello World".getBytes(StandardCharsets.UTF_8));
        // producer.send(message);

        // 批量消息
        Message message1 = new Message("one2oneTopic","one2oneTags","Hello World".getBytes(StandardCharsets.UTF_8));
        Message message2 = new Message("one2oneTopic","one2oneTags","Hello World".getBytes(StandardCharsets.UTF_8));
        List<Message> messages = new ArrayList<>();
        messages.add(message1);
        messages.add(message2);
        producer.send(messages);

        // 异步消息
        // producer.send(message, new SendCallback() {
        //     @Override
        //     public void onSuccess(SendResult sendResult) {
        //         log.info("消息发送成功：{}",sendResult);
        //     }
        //
        //     @Override
        //     public void onException(Throwable e) {
        //         log.info("消息发送失败：{}",e);
        //     }
        // });
        System.in.read();
        // 5.释放资源
        producer.shutdown();
    }
}
