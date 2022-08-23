package com.github.karlnicholas.merchloan.jms.queue;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.*;

import java.util.List;

@Slf4j
class QueueMessageHandler extends Thread implements Runnable {
    private final List<QueueMessage> messsageQueue;
    private final ClientSession clientSession;
    private final ClientProducer producer;
    private boolean run;


    public QueueMessageHandler(ClientSessionFactory clientSessionFactory, List<QueueMessage> messsageQueue, String sessionId) throws Exception {
        run = true;
        this.messsageQueue = messsageQueue;
        clientSession = clientSessionFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", sessionId);
        producer = clientSession.createProducer();
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
                    QueueMessage message = messsageQueue.remove(0);
                    messsageQueue.notifyAll();
                    message.getProducer().sendMessage(producer, message.getMessage());
                }
            } catch (InterruptedException ex) {
                if ( run ) ex.printStackTrace();
                Thread.currentThread().interrupt();
            } catch (ActiveMQException e) {
                log.error("QueueMessageHandler::run ", e);
            }
        }
    }
}
