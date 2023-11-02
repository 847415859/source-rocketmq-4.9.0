// package com.qiankun.test.rocketmq.consumer;
//
// import com.alibaba.csp.sentinel.Entry;
// import com.alibaba.csp.sentinel.EntryType;
// import com.alibaba.csp.sentinel.SphU;
// import com.alibaba.csp.sentinel.context.ContextUtil;
// import com.alibaba.csp.sentinel.slots.block.BlockException;
// import com.alibaba.csp.sentinel.slots.block.RuleConstant;
// import com.alibaba.csp.sentinel.slots.block.flow.FlowRule;
// import com.alibaba.csp.sentinel.slots.block.flow.FlowRuleManager;
// import com.qiankun.test.rocketmq.Default;
// import com.tk.rocketmq.constant.Default;
// import lombok.extern.slf4j.Slf4j;
// import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
// import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
// import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
// import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
// import org.apache.rocketmq.client.exception.MQClientException;
// import org.apache.rocketmq.common.message.MessageExt;
//
// import java.util.Collections;
// import java.util.List;
// import java.util.concurrent.atomic.AtomicInteger;
//
// /**
//  * @Description:
//  * @Date : 2023/02/24 10:06
//  * @Auther : tiankun
//  */
// @Slf4j
// public class FlowConsumer {
//
//     private static final String GROUP_NAME = "flowGroup";
//
//     private static final String TOPIC_NAME = "flow";
//
//     private static final String RESOURCE_KEY = GROUP_NAME +":"+ TOPIC_NAME;
//
//     private static AtomicInteger number = new AtomicInteger(1);
//
//     public static void main(String[] args) throws MQClientException {
//         // initFlowControlRule();
//         DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(GROUP_NAME);
//         consumer.subscribe(TOPIC_NAME,"*");
//         consumer.setNamesrvAddr(Default.NAME_SERVER_ADDRESS);
//         consumer.setConsumeThreadMin(1);
//         consumer.setConsumeThreadMin(1);
//
//         consumer.registerMessageListener(new MessageListenerConcurrently() {
//             @Override
//             public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
//                 for (MessageExt msg : msgs) {
//                     Entry entry = null;
//                     try {
//                         ContextUtil.enter(RESOURCE_KEY);
//                         entry = SphU.asyncEntry(RESOURCE_KEY, EntryType.OUT);
//                         log.info("消息：{}  详情：{}",number.getAndIncrement(),msg);
//                     } catch (BlockException e) {
//                         e.printStackTrace();
//                     }finally {
//                         entry.exit();
//                     }
//
//                 }
//                 return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//             }
//         });
//         consumer.start();
//     }
//
//
//     private static void initFlowControlRule() {
//         FlowRule rule = new FlowRule();
//         // 对应的 key 为 groupName:topicName
//         rule.setResource(RESOURCE_KEY);
//         // 使用QPS进行流控
//         rule.setGrade(RuleConstant.FLOW_GRADE_QPS);
//         rule.setCount(1);
//         rule.setLimitApp("default");
//         // 匀速器模式下，设置了 QPS 为 5，则请求每 200 ms 允许通过 1 个
//         rule.setControlBehavior(RuleConstant.CONTROL_BEHAVIOR_RATE_LIMITER);
//         // 如果更多的请求到达，这些请求会被置于虚拟的等待队列中。等待队列有一个 max timeout，如果请求预计的等待时间超过这个时间会直接被 block
//         // 在这里，timeout 为 5s
//         rule.setMaxQueueingTimeMs(5 * 1000);
//         FlowRuleManager.loadRules(Collections.singletonList(rule));
//     }
// }
