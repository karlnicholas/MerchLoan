package com.github.karlnicholas.merchloan.query.jms;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class QueueWaitingHandler {
    public static final int RESPONSE_TIMEOUT = 3000;
    public static final long TIMEOUT_MAX = 9_000_000_000L;
    private final ConcurrentMap<String, QueueWaiting> repliesWaiting;

    public QueueWaitingHandler() {
        repliesWaiting = new ConcurrentHashMap<>();
    }

    public void put(String responseKey) {
        repliesWaiting.put(responseKey, QueueWaiting.builder().nanoTime(System.nanoTime()).reply(null).build());
    }

    public Object getReply(String responseKey) {
        synchronized (repliesWaiting) {
            while (repliesWaiting.containsKey(responseKey) && repliesWaiting.get(responseKey).checkReply().isEmpty()) {
                try {
                    repliesWaiting.wait(RESPONSE_TIMEOUT);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                if (System.nanoTime() - repliesWaiting.get(responseKey).getNanoTime() > TIMEOUT_MAX) {
                    log.error("getReply timeout {}, {}", repliesWaiting, responseKey);
                    break;
                }
            }
        }
        return repliesWaiting.remove(responseKey).getReply();
    }

    public int getRepliesWaitingSize() {
        return repliesWaiting.size();
    }
    public void handleReply(String key, Object reply) {
        synchronized (repliesWaiting) {
            QueueWaiting rw = repliesWaiting.get(key);
            rw.setReply(reply);
            repliesWaiting.notifyAll();
        }
    }
}
