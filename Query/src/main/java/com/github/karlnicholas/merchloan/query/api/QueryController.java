package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import com.github.karlnicholas.merchloan.query.message.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PreDestroy;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequestMapping(value = "/api/query")
@Slf4j
public class QueryController {
    private final QueueMessageService queueMessageService;
    private final QueryServiceRequestProducer queryServiceRequestProducer;
    private final QueryAccountProducer queryAccountProducer;
    private final QueryLoanProducer queryLoanProducer;
    private final QueryStatementProducer queryStatementProducer;
    private final QueryStatementsProducer queryStatementsProducer;
    private final QueryCheckRequestProducer queryCheckRequestProducer;

    public QueryController(ServerLocator locator, MQConsumerUtils mqConsumerUtils, QueueMessageService queueMessageService) throws Exception {
        this.queueMessageService = queueMessageService;

        queryServiceRequestProducer = new QueryServiceRequestProducer(locator, mqConsumerUtils);
        queryAccountProducer = new QueryAccountProducer(locator, mqConsumerUtils);
        queryLoanProducer = new QueryLoanProducer(locator, mqConsumerUtils);
        queryStatementProducer = new QueryStatementProducer(locator, mqConsumerUtils);
        queryStatementsProducer = new QueryStatementsProducer(locator, mqConsumerUtils);
        queryCheckRequestProducer = new QueryCheckRequestProducer(locator, mqConsumerUtils);

        queueMessageService.initialize(locator, "Query");
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException, InterruptedException {
        queryServiceRequestProducer.close();
        queryAccountProducer.close();
        queryLoanProducer.close();
        queryStatementProducer.close();
        queryStatementsProducer.close();
        queryCheckRequestProducer.close();
        queueMessageService.close();
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
        queueMessageService.addMessage(queryCheckRequestProducer, Optional.of(responseKey), id);
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