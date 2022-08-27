package com.github.karlnicholas.merchloan.jms.queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
class QueueMessageHandler extends Thread implements Runnable {
    private final List<QueueMessage> messsageQueue;
    private final Channel producer;
    private boolean run;


    public QueueMessageHandler(Connection connection, List<QueueMessage> messsageQueue) throws Exception {
        run = true;
        this.messsageQueue = messsageQueue;
        this.producer = connection.createChannel();
    }

    public void stopHandler() {
        run = false;
        this.interrupt();
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
            }
        }
    }
}
