package com.github.karlnicholas.merchloan.jms.queue;

import lombok.Data;

import java.util.Optional;

@Data
public class QueueMessage {
    private final Object message;
    private final QueueMessageHandlerProducer producer;
    private final Optional<String> responseKeyOpt;

    public QueueMessage(Object message, QueueMessageHandlerProducer producer, Optional<String> responseKeyOpt) {
        this.message = message;
        this.producer = producer;
        this.responseKeyOpt = responseKeyOpt;
    }
}
