package com.github.karlnicholas.merchloan.jms.queue;

import com.rabbitmq.client.AMQP;

public class QueueMessage {
    private final Object message;
    private final AMQP.BasicProperties properties;
    private final QueueMessageHandlerProducer producer;

    public QueueMessage(QueueMessageHandlerProducer producer, AMQP.BasicProperties properties, Object message) {
        this.message = message;
        this.properties = properties;
        this.producer = producer;
    }

    public QueueMessageHandlerProducer getProducer() {
        return producer;
    }
    public AMQP.BasicProperties getProperties() {
        return properties;
    }
    public Object getMessage() {
        return message;
    }
}
