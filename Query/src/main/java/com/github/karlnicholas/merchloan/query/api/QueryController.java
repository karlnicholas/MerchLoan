package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessage;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import com.github.karlnicholas.merchloan.query.jms.QueueWaitingHandler;
import com.github.karlnicholas.merchloan.query.message.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.http.MediaType;
import org.springframework.util.SerializationUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PreDestroy;
import java.util.UUID;

@RestController
@RequestMapping(value = "/api/query")
@Slf4j
public class QueryController {
    private final ClientSession clientSession;
    private final SimpleString queryReplyQueue;
    private final QueueMessageService queueMessageService;
    private final QueryServiceRequestProducer queryServiceRequestProducer;
    private final QueryAccountProducer queryAccountProducer;
    private final QueryLoanProducer queryLoanProducer;
    private final QueryStatementProducer queryStatementProducer;
    private final QueryStatementsProducer queryStatementsProducer;
    private final QueryCheckRequestProducer queryCheckRequestProducer;
    private final QueueWaitingHandler queueWaitingHandler;

    public QueryController(ServerLocator locator, MQConsumerUtils mqConsumerUtils, QueueMessageService queueMessageService) throws Exception {
        this.queueMessageService = queueMessageService;
        queueWaitingHandler = new QueueWaitingHandler();

        clientSession = locator.createSessionFactory().createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "query-consumer");

        queryReplyQueue = SimpleString.toSimpleString("queryReply-" + UUID.randomUUID());

//        ReplyWaitingHandler replyWaitingHandler = new ReplyWaitingHandler();

        mqConsumerUtils.bindConsumer(clientSession, queryReplyQueue, true, message -> {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            queueWaitingHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });

        queryServiceRequestProducer = new QueryServiceRequestProducer(SimpleString.toSimpleString(mqConsumerUtils.getServicerequestQueryIdQueue()));
        queryAccountProducer = new QueryAccountProducer(SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryAccountIdQueue()));
        queryLoanProducer = new QueryLoanProducer(SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryLoanIdQueue()));
        queryStatementProducer = new QueryStatementProducer(SimpleString.toSimpleString(mqConsumerUtils.getStatementQueryStatementQueue()));
        queryStatementsProducer = new QueryStatementsProducer(SimpleString.toSimpleString(mqConsumerUtils.getStatementQueryStatementsQueue()));
        queryCheckRequestProducer = new QueryCheckRequestProducer(SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestCheckRequestQueue()));

        queueMessageService.initialize(locator, "query-producer-", 50);
        clientSession.start();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException, InterruptedException {
        queueMessageService.close();
        clientSession.close();
    }

    @GetMapping(value = "/request/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryRequestId(@PathVariable UUID id) throws Exception {
        log.debug("request: {}", id);
        return handleStringRequest(queryServiceRequestProducer, id);
    }

    @GetMapping(value = "/account/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryAccountId(@PathVariable UUID id) throws Exception {
        log.debug("account: {}", id);
        return handleStringRequest(queryAccountProducer, id);
    }

    @GetMapping(value = "/loan/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryLoanId(@PathVariable UUID id) throws Exception {
        log.debug("loan: {}", id);
        return handleStringRequest(queryLoanProducer, id);
    }

    @GetMapping(value = "/statement/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementId(@PathVariable UUID id) throws Exception {
        log.debug("statement: {}", id);
        return handleStringRequest(queryStatementProducer, id);
    }

    @GetMapping(value = "/statements/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementsId(@PathVariable UUID id) throws Exception {
        log.debug("statements: {}", id);
        return handleStringRequest(queryStatementsProducer, id);
    }

    private String handleStringRequest(QueueMessageHandlerProducer producer, UUID id) throws InterruptedException {
        String responseKey = UUID.randomUUID().toString();
        queueWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(queryReplyQueue);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
        QueueMessage queueMessage = new QueueMessage(producer, message);
        queueMessageService.addMessage(queueMessage);
        return queueWaitingHandler.getReply(responseKey).toString();
    }

    @GetMapping(value = "/checkrequests", produces = MediaType.APPLICATION_JSON_VALUE)
    public Boolean queryCheckRequests() throws Exception {
        log.debug("checkrequests");
        String responseKey = UUID.randomUUID().toString();
        queueWaitingHandler.put(responseKey);
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(queryReplyQueue);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        QueueMessage queueMessage = new QueueMessage(queryCheckRequestProducer, message);
        queueMessageService.addMessage(queueMessage);
        return (Boolean) queueWaitingHandler.getReply(responseKey);
    }
}