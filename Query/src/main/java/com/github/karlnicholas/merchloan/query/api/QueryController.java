package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import com.github.karlnicholas.merchloan.query.message.*;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ServerLocator;

import java.util.Optional;
import java.util.UUID;

@Path("api")
@RequestScoped
@Slf4j
public class QueryController {
    private final QueueMessageService queueMessageService;
    private final QueryServiceRequestProducer queryServiceRequestProducer;
    private final QueryAccountProducer queryAccountProducer;
    private final QueryLoanProducer queryLoanProducer;
    private final QueryStatementProducer queryStatementProducer;
    private final QueryStatementsProducer queryStatementsProducer;
    private final QueryCheckRequestProducer queryCheckRequestProducer;


    @Inject
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

    @GET
    @Path("request/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String queryRequestId(@PathParam("id") UUID id) throws Exception {
        log.debug("request: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryServiceRequestProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GET
    @Path("account/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String queryAccountId(@PathParam("id") UUID id) throws Exception {
        log.debug("account: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryAccountProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GET
    @Path("loan/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String queryLoanId(@PathParam("id") UUID id) throws Exception {
        log.debug("loan: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryLoanProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GET
    @Path("statement/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String queryStatementId(@PathParam("id") UUID id) throws Exception {
        log.debug("statement: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryStatementProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GET
    @Path("statements/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public String queryStatementsId(@PathParam("id") UUID id) throws Exception {
        log.debug("statements: {}", id);
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryCheckRequestProducer, Optional.of(responseKey), id);
        return queueMessageService.getReply(responseKey).toString();
    }
    @GET
    @Path("checkrequests")
    @Produces(MediaType.APPLICATION_JSON)
    public Boolean queryCheckRequests() throws Exception {
        log.debug("checkrequests");
        String responseKey = UUID.randomUUID().toString();
        queueMessageService.addMessage(queryCheckRequestProducer, Optional.of(responseKey), new byte[0]);
        return (Boolean)queueMessageService.getReply(responseKey);
    }
}