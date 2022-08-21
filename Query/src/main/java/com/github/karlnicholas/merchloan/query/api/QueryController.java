package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import com.github.karlnicholas.merchloan.query.message.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.http.MediaType;
import org.springframework.util.SerializationUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PreDestroy;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping(value = "/api/query")
@Slf4j
public class QueryController {
    private final ClientSessionFactory sessionFactory;
    private final ClientSession clientSession;
    private final QueueMessageService queueMessageService;
    private final QueryServiceRequestProducer queryServiceRequestProducer;
    private final QueryAccountProducer queryAccountProducer;
    private final QueryLoanProducer queryLoanProducer;
    private final QueryStatementProducer queryStatementProducer;
    private final QueryStatementsProducer queryStatementsProducer;
    public QueryController(ServerLocator locator, MQConsumerUtils mqConsumerUtils, QueueMessageService queueMessageService) throws Exception {
        this.queueMessageService = queueMessageService;

        sessionFactory = locator.createSessionFactory();
        clientSession = sessionFactory.createSession();
        SimpleString queryReplyQueue = SimpleString.toSimpleString("queryReply-" + UUID.randomUUID());

        ReplyWaitingHandler replyWaitingHandler = new ReplyWaitingHandler();
        mqConsumerUtils.bindConsumer(clientSession, queryReplyQueue, true, message -> {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            replyWaitingHandler.handleReply(message.getCorrelationID().toString(), SerializationUtils.deserialize(mo));
        });

        queryServiceRequestProducer = new QueryServiceRequestProducer(mqConsumerUtils, replyWaitingHandler, queryReplyQueue);
        queryAccountProducer = new QueryAccountProducer(mqConsumerUtils, replyWaitingHandler, queryReplyQueue);
        queryLoanProducer = new QueryLoanProducer(mqConsumerUtils, replyWaitingHandler, queryReplyQueue);
        queryStatementProducer = new QueryStatementProducer(mqConsumerUtils, replyWaitingHandler, queryReplyQueue);
        queryStatementsProducer = new QueryStatementsProducer(mqConsumerUtils, replyWaitingHandler, queryReplyQueue);
        queryCheckRequestProducer = new QueryCheckRequestProducer(mqConsumerUtils, replyWaitingHandler, queryReplyQueue);

        queueMessageService.initialize(locator, "Query");
        clientSession.start();
    }

    private final QueryCheckRequestProducer queryCheckRequestProducer;

    @PreDestroy
    public void preDestroy() throws ActiveMQException, InterruptedException {
        queueMessageService.close();
        clientSession.close();
        sessionFactory.close();
    }

    @GetMapping(value = "/request/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryRequestId(@PathVariable UUID id) throws Exception {
        log.debug("request: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryServiceRequestProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GetMapping(value = "/account/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryAccountId(@PathVariable UUID id) throws Exception {
        log.debug("account: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryAccountProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GetMapping(value = "/loan/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryLoanId(@PathVariable UUID id) throws Exception {
        log.debug("loan: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryLoanProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GetMapping(value = "/statement/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementId(@PathVariable UUID id) throws Exception {
        log.debug("statement: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryStatementProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GetMapping(value = "/statements/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementsId(@PathVariable UUID id) throws Exception {
        log.debug("statements: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryStatementsProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GetMapping(value = "/checkrequests", produces = MediaType.APPLICATION_JSON_VALUE)
    public Boolean queryCheckRequests() throws Exception {
        log.debug("checkrequests");
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryCheckRequestProducer, Optional.of(responseKey), new byte[0]);
        return (Boolean)queueMessageService.getReply(responseKey);
    }
}