package com.github.karlnicholas.merchloan.jms.queue;

import lombok.Data;
import org.apache.activemq.artemis.api.core.client.ClientMessage;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

public class QueueMessage {
    private final ClientMessage message;
    private final QueueMessageHandlerProducer producer;
    private final Function<String, Object> replyHandler;
    private Object reply;
    private boolean replySet;
    private static final long timeout = 9_000_000_000L;

    public QueueMessage(QueueMessageHandlerProducer producer, ClientMessage message, Function<String, Object> replyHandler) {
        this.message = message;
        this.producer = producer;
        this.replyHandler = replyHandler;
        this.reply = null;
        this.replySet = false;
    }

    public Optional<Function<String, Object>> getReplyHandler() {
        return Optional.ofNullable(replyHandler);
    }

    public Object getReply() throws InterruptedException {
        synchronized (this) {
            while (!replySet) {
                this.wait(timeout);
            }
            return reply;
        }
    }
    public ClientMessage getMessage() {
        return message;
    }
    public QueueMessageHandlerProducer getProducer() {
        return producer;
    }
    public void setReply(Object reply) {
        this.reply = reply;
    }
    public void setReplySet(boolean replySet) {
        this.replySet = replySet;
    }
}
