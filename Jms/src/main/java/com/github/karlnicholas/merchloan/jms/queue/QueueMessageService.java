package com.github.karlnicholas.merchloan.jms.queue;

import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class QueueMessageService {
    private static final int MAX_CAPACITY = 5;
    private List<QueueMessage> messsageQueue;
    private ReplyWaitingHandler replyWaitingHandler;

    public void initialize(ServerLocator locator, String queueName) throws Exception {
        replyWaitingHandler = new ReplyWaitingHandler();
        messsageQueue = new ArrayList<>();
        for ( int i = 0 ; i < MAX_CAPACITY; ++i) {
            Thread tConsumer = new Thread(new QueueMessageHandler(locator, queueName, messsageQueue));
            tConsumer.start();
        }
    }

    public void addMessage(QueueMessageHandlerProducer producer, String responseKey, Object data) throws InterruptedException {
        synchronized (messsageQueue) {
            while(messsageQueue.size() >= MAX_CAPACITY) {
                messsageQueue.wait();
            }
            QueueMessage queueMessage = new QueueMessage(data, producer);
            replyWaitingHandler.put(responseKey);
            messsageQueue.add(queueMessage);
            messsageQueue.notifyAll();
        }
    }

    public Object getReply(String responseKey) throws InterruptedException {
        return replyWaitingHandler.getReply(responseKey);
    }

}
