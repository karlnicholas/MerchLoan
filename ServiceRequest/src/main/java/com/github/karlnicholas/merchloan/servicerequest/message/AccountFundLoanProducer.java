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
import org.springframework.util.SerializationUtils;

@Slf4j
public class AccountFundLoanProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;

    public AccountFundLoanProducer(MQConsumerUtils mqConsumerUtils) {
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountFundingQueue());
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        FundLoan fundLoan = (FundLoan) data;
        log.debug("accountFundLoan: {}", fundLoan);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(fundLoan));
        producer.send(queue, message);
        return null;
    }
}
