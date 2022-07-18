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

import java.io.IOException;

@Slf4j
public class StatementStatementProducer implements QueueMessageHandlerProducer {
    private final MQConsumerUtils mqConsumerUtils;
    private final SimpleString queue;

    public StatementStatementProducer(MQConsumerUtils mqConsumerUtils) {
        this.mqConsumerUtils = mqConsumerUtils;
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getStatementStatementQueue());
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        try {
            StatementHeader statementHeader = (StatementHeader) data;
            log.debug("statementStatement: {}", statementHeader);
            ClientMessage message = clientSession.createMessage(false);
            mqConsumerUtils.serializeToMessage(message, statementHeader);
            producer.send(queue, message);
            return null;
        } catch (IOException e) {
            log.error("StatementStatementProducer ", e);
        }
        return null;
    }

    @Override
    public void close() {
        // nothing to close
    }
}
