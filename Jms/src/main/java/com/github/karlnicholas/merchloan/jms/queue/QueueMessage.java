package com.github.karlnicholas.merchloan.jms.queue;


import com.rabbitmq.client.Delivery;

public class QueueMessage {
    private final Delivery message;
    private final QueueMessageHandlerProducer producer;

    public QueueMessage(QueueMessageHandlerProducer producer, Delivery message) {
        this.message = message;
        this.producer = producer;
    }

    public Delivery getMessage() {
        return message;
    }
    public QueueMessageHandlerProducer getProducer() {
        return producer;
    }
}
