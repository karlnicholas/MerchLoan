package com.github.karlnicholas.merchloan.jms.queue;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

@Service
public class QueueMessageService {
    private List<QueueMessage> messsageQueue;
    private List<QueueMessageHandler> handlers;
    private int capacity;

    public void initialize(Connection connection, String queueName, int capacity) throws Exception {
        this.capacity = capacity;
        messsageQueue = new ArrayList<>();
        handlers = new ArrayList<>();
        for ( int i = 0 ; i < capacity; ++i) {
            QueueMessageHandler queueMessageHandler = new QueueMessageHandler(connection, messsageQueue);
            handlers.add(queueMessageHandler);
            queueMessageHandler.start();
        }
    }
    public void close() throws InterruptedException, IOException, TimeoutException {
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
