package com.github.karlnicholas.merchloan.jms;

import lombok.extern.slf4j.Slf4j;

import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Slf4j
public class ReplyWaitingHandler implements MessageListener {
    public static final int RESPONSE_TIMEOUT = 3000;
    public static final long TIMEOUT_MAX = 9_000_000_000L;
    private final ConcurrentMap<String, ReplyWaiting> repliesWaiting;

    public ReplyWaitingHandler() {
        repliesWaiting = new ConcurrentHashMap<>();
    }

    public void put(String responseKey) {
        repliesWaiting.put(responseKey, ReplyWaiting.builder().nanoTime(System.nanoTime()).reply(null).build());
    }

    public Object getReply(String responseKey) throws InterruptedException {
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

    @Override
    public void onMessage(Message message) {
        synchronized (repliesWaiting) {
            try {
                String corrId = message.getJMSCorrelationID();
                if ( corrId == null) log.error("null corrId");
                Serializable o = ((ObjectMessage) message).getObject();
                if ( o == null) log.error("null message object");
                ReplyWaiting rw = repliesWaiting.get(corrId);
                if ( rw == null ) log.error("RW not found: {}", repliesWaiting.toString());
                else rw.setReply(o);
                repliesWaiting.notifyAll();
            } catch (Exception e) {
                log.error("ReplyWaitingHandler::onMessage", e);
            }
        }
    }
}
