package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;

public interface QueueMessageHandlerProducer {
    void sendMessage(ClientProducer producer, ClientMessage message) throws ActiveMQException, InterruptedException;
}
