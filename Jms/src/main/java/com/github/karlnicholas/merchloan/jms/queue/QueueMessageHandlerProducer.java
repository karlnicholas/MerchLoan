package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.util.Optional;
import java.util.UUID;

public interface QueueMessageHandlerProducer {
    void sendMessage(ClientProducer producer, ClientMessage message) throws ActiveMQException, InterruptedException;
}
