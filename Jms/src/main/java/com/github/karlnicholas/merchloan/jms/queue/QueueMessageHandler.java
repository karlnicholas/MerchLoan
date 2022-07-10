package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

import java.util.List;

class QueueMessageHandler implements Runnable {
    private final List<QueueMessage> taskQueue;
    private final ClientSession clientSession;
    private final ClientProducer producer;


    public QueueMessageHandler(ServerLocator locator, String queueName, List<QueueMessage> sharedQueue) throws Exception {
        this.taskQueue = sharedQueue;
        clientSession = locator.createSessionFactory().createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", queueName);
        producer = clientSession.createProducer();
        clientSession.start();
    }

    @Override
    public void run() {
        while (true) {
            try {
                synchronized (taskQueue) {
                    while (taskQueue.isEmpty()) {
                        taskQueue.wait();
                    }
                    QueueMessage queueMessage = taskQueue.remove(0);
                    taskQueue.notifyAll();
                    queueMessage.getProducer().sendMessage(clientSession, producer, queueMessage.getMessage());
                }
            } catch (InterruptedException ex) {
                ex.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }
}
