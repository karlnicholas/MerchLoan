package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jmsmessage.FundLoan;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.io.IOException;

@Slf4j
public class AccountFundLoanProducer implements QueueMessageHandlerProducer {
    private final MQConsumerUtils mqConsumerUtils;
    private final SimpleString queue;

    public AccountFundLoanProducer(MQConsumerUtils mqConsumerUtils) {
        this.mqConsumerUtils = mqConsumerUtils;
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountFundingQueue());
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        try {
            FundLoan fundLoan = (FundLoan) data;
            log.debug("accountFundLoan: {}", fundLoan);
            ClientMessage message = clientSession.createMessage(false);
            mqConsumerUtils.serializeToMessage(message, fundLoan);
            producer.send(queue, message);
            return null;
        } catch (IOException e) {
            log.error("AccountFundLoanProducer ", e);
        }
        return null;
    }

    @Override
    public void close() {
        // nothing to close
    }
}
