package com.github.karlnicholas.merchloan.jms;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.springframework.util.SerializationUtils;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class ReplyWaitingHandler {
    public static final int RESPONSE_TIMEOUT = 3000;
    public static final long TIMEOUT_MAX = 9_000_000_000L;
    private final ConcurrentMap<UUID, ReplyWaiting> repliesWaiting;

    public ReplyWaitingHandler() {
        repliesWaiting = new ConcurrentHashMap<>();
    }

    public void put(UUID responseKey) {
        repliesWaiting.put(responseKey, ReplyWaiting.builder().nanoTime(System.nanoTime()).reply(null).build());
    }

    public Object getReply(UUID responseKey) throws InterruptedException {
        synchronized (repliesWaiting) {
            while (repliesWaiting.containsKey(responseKey) && repliesWaiting.get(responseKey).checkReply().isEmpty()) {
                repliesWaiting.wait(RESPONSE_TIMEOUT);
                if (System.nanoTime() - repliesWaiting.get(responseKey).getNanoTime() > TIMEOUT_MAX) {
                    log.error("getReply timeout");
                    break;
                }
            }
        }
        return repliesWaiting.remove(responseKey).getReply();
    }

    public void handleReplies(ClientMessage message) {
        synchronized (repliesWaiting) {
            UUID corrId = (UUID)message.getCorrelationID();
            repliesWaiting.get(corrId).setReply(SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array()));
            repliesWaiting.notifyAll();
        }
    }
}
