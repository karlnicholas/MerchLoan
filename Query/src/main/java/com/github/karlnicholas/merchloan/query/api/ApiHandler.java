package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import com.github.karlnicholas.merchloan.query.message.*;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
import java.util.Optional;
import java.util.UUID;

@Component
public class ApiHandler {
    private final QueueMessageService queueMessageService;
    private final QueryServiceRequestProducer queryServiceRequestProducer;
    private final QueryAccountProducer queryAccountProducer;
    private final QueryLoanProducer queryLoanProducer;
    private final QueryStatementProducer queryStatementProducer;
    private final QueryStatementsProducer queryStatementsProducer;
    private final QueryCheckRequestProducer queryCheckRequestProducer;

    public ApiHandler(ServerLocator locator, MQConsumerUtils mqConsumerUtils, QueueMessageService queueMessageService) throws Exception {
        this.queueMessageService = queueMessageService;
        ReplyWaitingHandler replyWaitingHandler = new ReplyWaitingHandler();

        queryServiceRequestProducer = new QueryServiceRequestProducer(locator, mqConsumerUtils);
        queryAccountProducer = new QueryAccountProducer(locator, mqConsumerUtils);
        queryLoanProducer = new QueryLoanProducer(locator, mqConsumerUtils);
        queryStatementProducer = new QueryStatementProducer(locator, mqConsumerUtils);
        queryStatementsProducer = new QueryStatementsProducer(locator, mqConsumerUtils);
        queryCheckRequestProducer = new QueryCheckRequestProducer(locator, mqConsumerUtils);

        queueMessageService.initialize(locator, replyWaitingHandler, "Query");
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

    public Mono<ServerResponse> getRequest(ServerRequest serverRequest) {
        return processServerRequest(serverRequest, queryServiceRequestProducer);
    }

    private Mono<ServerResponse> processServerRequest(ServerRequest serverRequest, QueueMessageHandlerProducer queueMessageHandlerProducer) {
        return ServerResponse.ok()
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(UUID.fromString(serverRequest.pathVariable("id")))
                        .map(id -> {
                            try {
                                String responseKey = UUID.randomUUID().toString();
                                queueMessageService.addMessage(queueMessageHandlerProducer, Optional.of(responseKey), id);
                                return queueMessageService.getReply(responseKey).toString();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new ApiException(e);
                            }
                        }), String.class);
    }

    public Mono<ServerResponse> getAccount(ServerRequest serverRequest) {
        return processServerRequest(serverRequest, queryAccountProducer);
    }

    public Mono<ServerResponse> getLoan(ServerRequest serverRequest) {
        return processServerRequest(serverRequest, queryLoanProducer);
    }

    public Mono<ServerResponse> getStatement(ServerRequest serverRequest) {
        return processServerRequest(serverRequest, queryStatementProducer);
    }

    public Mono<ServerResponse> getStatements(ServerRequest serverRequest) {
        return processServerRequest(serverRequest, queryStatementsProducer);
    }

    public Mono<ServerResponse> getCheckRequests() {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(
                Mono.fromSupplier(() -> {
                    try {
                        String responseKey = UUID.randomUUID().toString();
                        queueMessageService.addMessage(queryCheckRequestProducer, Optional.of(responseKey), new byte[0]);
                        return (Boolean) queueMessageService.getReply(responseKey);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ApiException(e);
                    }
                })
                , Boolean.class);
    }
}
