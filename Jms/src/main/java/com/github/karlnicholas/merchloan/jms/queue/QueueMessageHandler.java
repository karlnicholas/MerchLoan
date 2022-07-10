package com.github.karlnicholas.merchloan.jms.queue;

import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

import java.util.List;

class QueueMessageHandler extends Thread implements Runnable {
    private final List<QueueMessage> messsageQueue;
    private final ClientSession clientSession;
    private final ClientProducer producer;
    private boolean run;


    public QueueMessageHandler(ServerLocator locator, String queueName, List<QueueMessage> messsageQueue) throws Exception {
        run = true;
        this.messsageQueue = messsageQueue;
        clientSession = locator.createSessionFactory().createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", queueName);
        producer = clientSession.createProducer();
        clientSession.start();
    }

    public void stopHandler() {
        run = false;
        this.interrupt();
    }
    public void close() throws ActiveMQException {
        clientSession.close();
    }

    @Override
    public void run() {
        while (run) {
            try {
                synchronized (messsageQueue) {
                    while (messsageQueue.isEmpty()) {
                        messsageQueue.wait();
                    }
                    QueueMessage queueMessage = messsageQueue.remove(0);
                    messsageQueue.notifyAll();
                    queueMessage.getProducer().sendMessage(clientSession, producer, queueMessage.getMessage(), queueMessage.getResponseKeyOpt());
                }
            } catch (InterruptedException ex) {
                if ( run ) ex.printStackTrace();
                Thread.currentThread().interrupt();
            }
        }
    }
}
