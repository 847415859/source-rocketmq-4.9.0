package com.qiankun.test.rocketmq.consumer;

import com.qiankun.test.rocketmq.Default;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @Description:
 * @Date : 2023/02/21 18:46
 * @Auther : tiankun
 */
@Slf4j
public class PullConsumer {

    public static void main(String[] args) throws MQClientException, MQBrokerException, RemotingException, InterruptedException, IOException {
        // 1.创建消费者对象 Consumer
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer("one2oneGroup");
        // 2.设定接受的命名服务器
        consumer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
        // 3.启动服务接受消息
        consumer.start();
        // 4.获取该Topic下所有队列
        Set<MessageQueue> messageQueueSet = consumer.fetchSubscribeMessageQueues("one2oneTopic");
        // 5.拉取消息
        for (MessageQueue messageQueue : messageQueueSet) {
            // 参数1: 消息队列
            // 参数2：标签tag过滤
            // 参数3：消息的偏移量,从当前位置开始消费
            // 参数4：每次最多拉取多少消息
            PullResult pullResult = consumer.pull(messageQueue, "*", 0, 10);
            List<MessageExt> msgFoundList = pullResult.getMsgFoundList();
            for (MessageExt messageExt : msgFoundList) {
                log.info("接收到的消息为：{}",messageExt);
                log.info("消息内容：{}",new String(messageExt.getBody()));
            }
        }
        System.in.read();
    }
}
