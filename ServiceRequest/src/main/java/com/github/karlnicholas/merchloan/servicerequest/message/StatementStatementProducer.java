package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeaderWork;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.util.SerializationUtils;

@Slf4j
public class StatementStatementProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;

    public StatementStatementProducer(MQConsumerUtils mqConsumerUtils) {
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountStatementStatementHeaderQueue());
    }

    @Override
    public void sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork) data;
        log.debug("statementStatement: {}", statementHeaderWork);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeaderWork));
        producer.send(queue, message);
    }
}
