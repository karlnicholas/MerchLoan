package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jmsmessage.FundLoan;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.util.SerializationUtils;

@Slf4j
public class AccountFundLoanProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;

    public AccountFundLoanProducer(SimpleString queue) {
        this.queue = queue;
    }

    @Override
    public void sendMessage(ClientProducer producer, ClientMessage message) throws ActiveMQException {
        producer.send(queue, message);
    }
}
