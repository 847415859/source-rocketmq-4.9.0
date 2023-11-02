package com.qiankun.test.rocketmq.producer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 单生产者多消费者  One2More
 */
@Slf4j
public class One2MoreProducer {
    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException {
        // 1.创建生产者对象
        DefaultMQProducer producer = new DefaultMQProducer("one2moreGroup");
        // 设置超时时间
        producer.setSendMsgTimeout(30000);
        // 2.设置NameServer地址
        producer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        // 3.开启生产者服务
        producer.start();
        // 4.发送消息
        List<Message> messageList = new ArrayList<>();
        for (int i = 0; i < 30; i++) {
            Message msg = new Message("one2moreTopic","one2moreTags",("单生产者对多消费者"+i).getBytes(StandardCharsets.UTF_8));
            messageList.add(msg);
        }
        producer.send(messageList);
        // 5.关闭资源
        producer.shutdown();
    }
}
