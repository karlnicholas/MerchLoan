package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class QueueMessageService {
    private List<QueueMessage> messsageQueue;
    private List<QueueMessageHandler> handlers;
    private int capacity;

    public ClientSessionFactory initialize(ServerLocator locator, String queueName, int capacity) throws Exception {
        this.capacity = capacity;
        messsageQueue = new ArrayList<>();
        ClientSessionFactory clientSessionFactory = locator.createSessionFactory();
        handlers = new ArrayList<>();
        for ( int i = 0 ; i < capacity; ++i) {
            QueueMessageHandler queueMessageHandler = new QueueMessageHandler(clientSessionFactory, messsageQueue, queueName+(i+1));
            handlers.add(queueMessageHandler);
            queueMessageHandler.start();
        }
        return clientSessionFactory;
    }
    public void close() throws InterruptedException, ActiveMQException {
        for ( QueueMessageHandler queueMessageHandler: handlers) {
            queueMessageHandler.stopHandler();
            queueMessageHandler.join();
            queueMessageHandler.close();
        }
    }

    public void addMessage(QueueMessage message) throws InterruptedException {
        synchronized (messsageQueue) {
            while(messsageQueue.size() >= capacity) {
                messsageQueue.wait();
            }
            messsageQueue.add(message);
            messsageQueue.notifyAll();
        }
    }

}
