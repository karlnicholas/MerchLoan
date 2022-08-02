package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

public interface QueueMessageHandlerProducer {
    Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException, InterruptedException;
    void close() throws ActiveMQException;
}
