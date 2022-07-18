package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jmsmessage.CreateAccount;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;

import java.io.IOException;

@Slf4j
public class AccountCreateAccountProducer implements QueueMessageHandlerProducer {
    private final MQConsumerUtils mqConsumerUtils;
    private final SimpleString queue;

    public AccountCreateAccountProducer(MQConsumerUtils mqConsumerUtils) {
        this.mqConsumerUtils = mqConsumerUtils;
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountCreateAccountQueue());
    }

    @Override
    public Object sendMessage(ClientSession clientSession, ClientProducer producer, Object data) throws ActiveMQException {
        try {
            CreateAccount createAccount = (CreateAccount) data;
            log.debug("accountCreateAccount: {}", createAccount);
            ClientMessage message = clientSession.createMessage(false);
            mqConsumerUtils.serializeToMessage(message, createAccount);
            producer.send(queue, message);
            return null;
        } catch (IOException e) {
            log.error("AccountCreateAccountProducer ", e);
        }
        return null;
    }

    @Override
    public void close() {
        // nothing to close
    }
}
