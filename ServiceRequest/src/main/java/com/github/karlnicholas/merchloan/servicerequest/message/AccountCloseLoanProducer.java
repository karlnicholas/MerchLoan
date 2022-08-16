package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jmsmessage.CloseLoan;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.util.SerializationUtils;

@Slf4j
public class AccountCloseLoanProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;

    public AccountCloseLoanProducer(MQConsumerUtils mqConsumerUtils) {
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountCloseLoanQueue());
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        CloseLoan closeLoan = (CloseLoan) data;
        log.debug("accountCloseLoan: {}", closeLoan);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(closeLoan));
        producer.send(queue, message);
        return null;
    }

}
