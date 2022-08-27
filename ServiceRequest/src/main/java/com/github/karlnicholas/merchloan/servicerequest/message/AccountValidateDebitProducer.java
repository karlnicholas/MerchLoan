package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;

import org.springframework.util.SerializationUtils;

import java.io.IOException;

@Slf4j
public class AccountValidateDebitProducer implements QueueMessageHandlerProducer {
    private final String exchange;
    private final String queue;

    public AccountValidateDebitProducer(String exchange, String queue) {
        this.exchange = exchange;
        this.queue = queue;
    }
    @Override
    public void sendMessage(Channel producer, AMQP.BasicProperties properties, Object message) throws IOException {
        producer.basicPublish(exchange, queue, properties, SerializationUtils.serialize(message));
    }

}
