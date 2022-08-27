package com.github.karlnicholas.merchloan.jms.queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

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

    public void close() throws IOException, TimeoutException {
        producer.close();
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
                    message.getProducer().sendMessage(producer, message.getProperties(), message.getMessage());
                }
            } catch (InterruptedException ex) {
                if ( run ) ex.printStackTrace();
                Thread.currentThread().interrupt();
            } catch (IOException e) {
                if ( run ) e.printStackTrace();
            }
        }
    }
}
