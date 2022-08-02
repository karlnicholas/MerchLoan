package com.github.karlnicholas.merchloan.jms.queue;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

import java.util.List;

@Slf4j
class QueueMessageHandler extends Thread implements Runnable {
    private final List<QueueMessage> messsageQueue;
    private final ClientSessionFactory sessionFactory;
    private final ClientSession clientSession;
    private final ClientProducer producer;
    private final QueueWaitingHandler queueWaitingHandler;
    private boolean run;


    public QueueMessageHandler(ServerLocator locator, String queueName, List<QueueMessage> messsageQueue, QueueWaitingHandler queueWaitingHandler) throws Exception {
        this.queueWaitingHandler = queueWaitingHandler;
        run = true;
        this.messsageQueue = messsageQueue;
        sessionFactory = locator.createSessionFactory();
        clientSession = sessionFactory.createSession();
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
        sessionFactory.close();
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
                    Object reply = queueMessage.getProducer().sendMessage(clientSession, producer, queueMessage.getMessage());
                    queueMessage.getResponseKeyOpt().ifPresent(key-> queueWaitingHandler.handleReply(key, reply));
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
