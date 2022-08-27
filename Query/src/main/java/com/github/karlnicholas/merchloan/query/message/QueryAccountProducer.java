package com.github.karlnicholas.merchloan.query.message;

import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.SerializationUtils;

import java.io.IOException;

@Slf4j
public class QueryAccountProducer implements QueueMessageHandlerProducer {
    private final String exchange;
    private final String queue;

    public QueryAccountProducer(String exchange, String queue) {
        this.exchange = exchange;
        this.queue = queue;
    }

    @Override
    public void sendMessage(Channel producer, BasicProperties properties, Object message) throws IOException {
        producer.basicPublish(exchange, queue, properties, SerializationUtils.serialize(message));
    }

}
