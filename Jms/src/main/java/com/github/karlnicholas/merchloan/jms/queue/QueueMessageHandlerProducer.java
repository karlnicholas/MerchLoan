package com.github.karlnicholas.merchloan.jms.queue;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;

import java.io.IOException;

public interface QueueMessageHandlerProducer {
    void sendMessage(Channel producer, BasicProperties properties, Object message) throws InterruptedException, IOException;
}
