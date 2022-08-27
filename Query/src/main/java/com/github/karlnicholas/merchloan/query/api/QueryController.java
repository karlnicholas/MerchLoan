package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessage;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import com.github.karlnicholas.merchloan.query.jms.QueueWaitingHandler;
import com.github.karlnicholas.merchloan.query.message.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.util.SerializationUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping(value = "/api/query")
@Slf4j
public class QueryController {
    private final List<Channel> consumerSessions;
    private final Channel producerSession;
    private final String queryReplyQueue;
    private final QueueMessageService queueMessageService;
    private final QueryServiceRequestProducer queryServiceRequestProducer;
    private final QueryAccountProducer queryAccountProducer;
    private final QueryLoanProducer queryLoanProducer;
    private final QueryStatementProducer queryStatementProducer;
    private final QueryStatementsProducer queryStatementsProducer;
    private final QueryCheckRequestProducer queryCheckRequestProducer;
    private final QueueWaitingHandler queueWaitingHandler;

    public QueryController(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils, QueueMessageService queueMessageService) throws Exception {
        this.queueMessageService = queueMessageService;
        queueWaitingHandler = new QueueWaitingHandler();

        consumerSessions = new ArrayList<>();
        queryReplyQueue = "queryReply-" + UUID.randomUUID();

        Connection connection = connectionFactory.newConnection();
        Channel consumerSession = connection.createChannel();
        consumerSessions.add(consumerSession);

        // Connection connection, String exchange, String queueName, boolean exclusive, DeliverCallback deliverCallback
        mqConsumerUtils.bindConsumer(consumerSession, mqConsumerUtils.getExchange(), queryReplyQueue, true, (consumerTag, message) -> {
            queueWaitingHandler.handleReply(message.getProperties().getCorrelationId(), SerializationUtils.deserialize(message.getBody()));
        });

        queryServiceRequestProducer = new QueryServiceRequestProducer(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueryIdQueue());
        queryAccountProducer = new QueryAccountProducer(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryAccountIdQueue());
        queryLoanProducer = new QueryLoanProducer(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryLoanIdQueue());
        queryStatementProducer = new QueryStatementProducer(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementQueryStatementQueue()));
        queryStatementsProducer = new QueryStatementsProducer(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementQueryStatementsQueue());
        queryCheckRequestProducer = new QueryCheckRequestProducer(mqConsumerUtils.getExchange(), mqConsumerUtils.getServiceRequestCheckRequestQueue());

        producerSession = queueMessageService.initialize(connectionFactory, "query-producer-", 100).createSession();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException, InterruptedException {
        queueMessageService.close();
        consumerSessions.forEach(s-> {
            try {
                s.close();
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        });
        producerSession.close();
    }

    @GetMapping(value = "/request/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryRequestId(@PathVariable UUID id) throws Exception {
        log.debug("request: {}", id);
        return handleRequest(queryServiceRequestProducer, id).toString();
    }

    @GetMapping(value = "/account/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryAccountId(@PathVariable UUID id) throws Exception {
        log.debug("account: {}", id);
        return handleRequest(queryAccountProducer, id).toString();
    }

    int max = 0;
    @GetMapping(value = "/loan/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryLoanId(@PathVariable UUID id) throws Exception {
        log.debug("loan: {}", id);
        return handleRequest(queryLoanProducer, id).toString();
    }

    @GetMapping(value = "/statement/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementId(@PathVariable UUID id) throws Exception {
        log.debug("statement: {}", id);
        return handleRequest(queryStatementProducer, id).toString();
    }

    @GetMapping(value = "/statements/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementsId(@PathVariable UUID id) throws Exception {
        log.debug("statements: {}", id);
        return handleRequest(queryStatementsProducer, id).toString();
    }

    @GetMapping(value = "/checkrequests", produces = MediaType.APPLICATION_JSON_VALUE)
    public Boolean queryCheckRequests() throws Exception {
        log.debug("checkrequests");
        return (Boolean) handleRequest(queryCheckRequestProducer, new byte[0]);
    }

    private Object handleRequest(QueueMessageHandlerProducer producer, Object data) throws InterruptedException {
        String responseKey = UUID.randomUUID().toString();
        queueWaitingHandler.put(responseKey);
        ClientMessage message = producerSession.createMessage(false);
        message.setCorrelationID(responseKey);
        message.setReplyTo(queryReplyQueue);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(data));
        QueueMessage queueMessage = new QueueMessage(producer, message);
        queueMessageService.addMessage(queueMessage);
        return queueWaitingHandler.getReply(responseKey);
    }

}