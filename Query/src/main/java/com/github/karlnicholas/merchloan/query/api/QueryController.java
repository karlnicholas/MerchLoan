package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.rabbitmq.client.AMQP;
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
import java.io.IOException;
import java.util.UUID;

@RestController
@RequestMapping(value = "/api/query")
@Slf4j
public class QueryController {
    private final Connection consumerConnection;
    private final Connection producerConnection;
    private final MQConsumerUtils mqConsumerUtils;
    private final String queryReplyQueue;
    private final Channel queryServiceRequestProducer;
    private final Channel queryAccountProducer;
    private final Channel queryLoanProducer;
    private final Channel queryStatementProducer;
    private final Channel queryStatementsProducer;
    private final Channel queryCheckRequestProducer;
    private final ReplyWaitingHandler replyWaitingHandler;

    public QueryController(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) throws Exception {
        this.mqConsumerUtils = mqConsumerUtils;
        replyWaitingHandler = new ReplyWaitingHandler();

        queryReplyQueue = "queryReply-" + UUID.randomUUID();

        consumerConnection = connectionFactory.newConnection();

        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), queryReplyQueue, true, (consumerTag, message) -> {
            replyWaitingHandler.handleReply(message.getProperties().getCorrelationId(), SerializationUtils.deserialize(message.getBody()));
        });

        producerConnection = connectionFactory.newConnection();
        queryServiceRequestProducer = producerConnection.createChannel();
        queryAccountProducer = producerConnection.createChannel();
        queryLoanProducer = producerConnection.createChannel();
        queryStatementProducer = producerConnection.createChannel();
        queryStatementsProducer = producerConnection.createChannel();
        queryCheckRequestProducer = producerConnection.createChannel();
    }

    @PreDestroy
    public void preDestroy() throws IOException {
        consumerConnection.close();
        producerConnection.close();
    }

    @GetMapping(value = "/request/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryRequestId(@PathVariable UUID id) throws Exception {
        log.debug("request: {}", id);
        return handleRequest(queryServiceRequestProducer, mqConsumerUtils.getServicerequestQueryIdQueue(), id).toString();
    }

    @GetMapping(value = "/account/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryAccountId(@PathVariable UUID id) throws Exception {
        log.debug("account: {}", id);
        return handleRequest(queryAccountProducer, mqConsumerUtils.getAccountQueryAccountIdQueue(), id).toString();
    }

    int max = 0;
    @GetMapping(value = "/loan/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryLoanId(@PathVariable UUID id) throws Exception {
        log.debug("loan: {}", id);
        return handleRequest(queryLoanProducer, mqConsumerUtils.getAccountQueryLoanIdQueue(), id).toString();
    }

    @GetMapping(value = "/statement/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementId(@PathVariable UUID id) throws Exception {
        log.debug("statement: {}", id);
        return handleRequest(queryStatementProducer, mqConsumerUtils.getStatementQueryStatementQueue(), id).toString();
    }

    @GetMapping(value = "/statements/{id}", produces = MediaType.APPLICATION_JSON_VALUE)
    public String queryStatementsId(@PathVariable UUID id) throws Exception {
        log.debug("statements: {}", id);
        return handleRequest(queryStatementsProducer, mqConsumerUtils.getStatementQueryStatementsQueue(), id).toString();
    }

    @GetMapping(value = "/checkrequests", produces = MediaType.APPLICATION_JSON_VALUE)
    public Boolean queryCheckRequests() throws Exception {
        log.debug("checkrequests");
        return (Boolean) handleRequest(queryCheckRequestProducer, mqConsumerUtils.getServiceRequestCheckRequestQueue(), new byte[0]);
    }

    private Object handleRequest(Channel producer, String queue, Object data) throws IOException, InterruptedException {
        String responseKey = UUID.randomUUID().toString();
        replyWaitingHandler.put(responseKey);
        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder().correlationId(responseKey).replyTo(queryReplyQueue).build();
        producer.basicPublish(mqConsumerUtils.getExchange(), queue, properties, SerializationUtils.serialize(data));
        return replyWaitingHandler.getReply(responseKey);
    }

}