package com.github.karlnicholas.merchloan.jms.queue;

import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class QueueMessageService {
    private static final int MAX_CAPACITY = 5;
    private List<QueueMessage> messsageQueue;
    private ReplyWaitingHandler replyWaitingHandler;
    private List<QueueMessageHandler> handlers;

    public void initialize(ServerLocator locator, ReplyWaitingHandler replyWaitingHandler, String queueName) throws Exception {
        this.replyWaitingHandler = replyWaitingHandler;
        messsageQueue = new ArrayList<>();
        handlers = new ArrayList<>();
        for ( int i = 0 ; i < MAX_CAPACITY; ++i) {
            QueueMessageHandler queueMessageHandler = new QueueMessageHandler(locator, queueName, messsageQueue, replyWaitingHandler);
            handlers.add(queueMessageHandler);
            queueMessageHandler.start();
        }
    }
    public void close() throws InterruptedException, ActiveMQException {
        for ( QueueMessageHandler queueMessageHandler: handlers) {
            queueMessageHandler.stopHandler();
            queueMessageHandler.join();
            queueMessageHandler.close();
        }
    }

    public void addMessage(QueueMessageHandlerProducer producer, Optional<String> responseKeyOpt, Object data) throws InterruptedException {
        synchronized (messsageQueue) {
            while(messsageQueue.size() >= MAX_CAPACITY) {
                messsageQueue.wait();
            }
            responseKeyOpt.ifPresent(replyWaitingHandler::put);
            QueueMessage queueMessage = new QueueMessage(data, producer, responseKeyOpt);
            messsageQueue.add(queueMessage);
            messsageQueue.notifyAll();
        }
    }

    public Object getReply(String responseKey) throws InterruptedException {
        return replyWaitingHandler.getReply(responseKey);
    }
}
