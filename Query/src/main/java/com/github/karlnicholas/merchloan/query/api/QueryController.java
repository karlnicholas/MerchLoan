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
import java.util.concurrent.ThreadLocalRandom;

@RestController
@RequestMapping(value = "/api/query")
@Slf4j
public class QueryController {
    private final ClientSession consumerSession;
    private final ClientSession producerSession;
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

        consumerSession = locator.createSessionFactory().createSession();
        consumerSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        consumerSession.addMetaData("jms-client-id", "query-consumer");

        queryReplyQueue = SimpleString.toSimpleString("queryReply-" + UUID.randomUUID());

        mqConsumerUtils.bindConsumer(consumerSession, queryReplyQueue, true, message -> {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            queueWaitingHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });
        consumerSession.start();

        queryServiceRequestProducer = new QueryServiceRequestProducer(SimpleString.toSimpleString(mqConsumerUtils.getServicerequestQueryIdQueue()));
        queryAccountProducer = new QueryAccountProducer(SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryAccountIdQueue()));
        queryLoanProducer = new QueryLoanProducer(SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryLoanIdQueue()));
        queryStatementProducer = new QueryStatementProducer(SimpleString.toSimpleString(mqConsumerUtils.getStatementQueryStatementQueue()));
        queryStatementsProducer = new QueryStatementsProducer(SimpleString.toSimpleString(mqConsumerUtils.getStatementQueryStatementsQueue()));
        queryCheckRequestProducer = new QueryCheckRequestProducer(SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestCheckRequestQueue()));

        producerSession = queueMessageService.initialize(locator, "query-producer-", 10).createSession();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException, InterruptedException {
        queueMessageService.close();
        consumerSession.close();
        producerSession.close();
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

    int max = 0;
    @GetMapping(value = "/loan/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryLoanId(@PathVariable UUID id) throws Exception {
        log.debug("loan: {}", id);
//        String responseKey = UUID.randomUUID().toString();
//        queueWaitingHandler.put(responseKey);
//        ClientMessage message = producerSession.createMessage(false);
//        message.setCorrelationID(responseKey);
//        message.setReplyTo(queryReplyQueue);
//        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(id));
//        QueueMessage queueMessage = new QueueMessage(queryLoanProducer, message);
//        queueMessageService.addMessage(queueMessage);
//        Object result = queueWaitingHandler.getReply(responseKey).toString();
//        int s = queueWaitingHandler.getRepliesWaitingSize();
//        if ( s > max) max = s;
//        if (ThreadLocalRandom.current().nextInt(50) == 0 ) {
//            log.info("queryLoanId repliesWaitingSize: {} {}", max, s);
//        }
//        return (String) result;
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
        ClientMessage message = producerSession.createMessage(false);
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
        ClientMessage message = producerSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(queryReplyQueue);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(new byte[0]));
        QueueMessage queueMessage = new QueueMessage(queryCheckRequestProducer, message);
        queueMessageService.addMessage(queueMessage);
        return (Boolean) queueWaitingHandler.getReply(responseKey);
    }
}