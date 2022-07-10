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
import org.springframework.util.SerializationUtils;

import java.util.Optional;

@Slf4j
public class AccountCreateAccountProducer implements QueueMessageHandlerProducer {
    private final SimpleString queue;

    public AccountCreateAccountProducer(MQConsumerUtils mqConsumerUtils, SimpleString replyQueue) {
        this.queue = SimpleString.toSimpleString(mqConsumerUtils.getAccountCreateAccountQueue());
    }
    @Override
    public void sendMessage(ClientSession clientSession, ClientProducer producer, Object data, Optional<String> responseKeyOpt) {
        CreateAccount createAccount = (CreateAccount) data;
        log.debug("accountCreateAccount: {}", createAccount);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(createAccount));
        try {
            producer.send(queue, message, null);
        } catch (ActiveMQException e) {
            log.error("AccountCreateAccountProducer", e);
        }
    }
}
