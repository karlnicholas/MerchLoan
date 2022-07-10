package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jmsmessage.StatementHeader;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.util.SerializationUtils;

import java.util.Optional;

@Slf4j
public class StatementStatementProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;

    public StatementStatementProducer(MQConsumerUtils mqConsumerUtils, SimpleString replyQueue) {
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getStatementStatementQueue());
    }

    @Override
    public void sendMessage(ClientSession clientSession, ClientProducer producer, Object data, Optional<String> responseKeyOpt) {
        StatementHeader statementHeader = (StatementHeader) data;
        log.debug("statementStatement: {}", statementHeader);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        try {
            producer.send(queue, message, null);
        } catch (ActiveMQException e) {
            log.error("StatementStatementProducer", e);
        }
    }
}
