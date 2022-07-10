package com.github.karlnicholas.merchloan.jms.queue;

import lombok.Data;

@Data
public class QueueMessage {
    private final Object message;
    private final QueueMessageHandlerProducer producer;
    private final String responseKey;

    public QueueMessage(Object message, QueueMessageHandlerProducer producer, String responseKey) {
        this.message = message;
        this.producer = producer;
        this.responseKey = responseKey;
    }
}
