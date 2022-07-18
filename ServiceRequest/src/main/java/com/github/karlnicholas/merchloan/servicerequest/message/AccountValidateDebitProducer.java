package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jmsmessage.DebitLoan;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.io.IOException;

@Slf4j
public class AccountValidateDebitProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;
    private final MQConsumerUtils mqConsumerUtils;

    public AccountValidateDebitProducer(MQConsumerUtils mqConsumerUtils) {
        this.mqConsumerUtils = mqConsumerUtils;
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountValidateDebitQueue());
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        try {
            DebitLoan debitLoan = (DebitLoan) data;
            log.debug("accountValidateDebit: {}", debitLoan);
            ClientMessage message = clientSession.createMessage(false);
            mqConsumerUtils.serializeToMessage(message, debitLoan);
            producer.send(queue, message);
            return null;
        } catch (IOException e) {
            log.error("AccountValidateDebitProducer ", e);
        }
        return null;
    }
    @Override
    public void close() {
        // nothing to close
    }
}
