package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;

@Slf4j
public class AccountCloseLoanProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;

    public AccountCloseLoanProducer(SimpleString queue) {
        this.queue = queue;
    }

    @Override
    public void sendMessage(ClientProducer producer, ClientMessage message) throws ActiveMQException {
        producer.send(queue, message);
    }
}
