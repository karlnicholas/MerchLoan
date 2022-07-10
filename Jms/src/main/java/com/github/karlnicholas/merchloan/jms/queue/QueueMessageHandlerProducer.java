package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.util.Optional;

public interface QueueMessageHandlerProducer {
    void sendMessage(ClientSession clientSession, ClientProducer producer, Object data, Optional<String> responseKeyOpt);
}
