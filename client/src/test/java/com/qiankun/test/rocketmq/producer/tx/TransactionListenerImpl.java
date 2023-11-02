package com.qiankun.test.rocketmq.producer.tx;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

@Slf4j
public class TransactionListenerImpl implements TransactionListener {

    /**
     *
     * @param msg
     * @param o
     * @return
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object o) {
       log.info("执行本地事务: Message:{}  arg:{}",msg,o);
        if (StringUtils.equals("TagA", msg.getTags())) {
            return LocalTransactionState.COMMIT_MESSAGE;
        } else if (StringUtils.equals("TagB", msg.getTags())) {
            return LocalTransactionState.ROLLBACK_MESSAGE;
        } else {
            return LocalTransactionState.UNKNOW;
        }
    }

    /**
     * 事务的补偿
     * @param messageExt
     * @return
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        log.info("事务补偿：{}",messageExt);
        return LocalTransactionState.UNKNOW;
    }
}
