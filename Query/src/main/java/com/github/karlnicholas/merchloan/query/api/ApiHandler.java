package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageHandlerProducer;
import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import com.github.karlnicholas.merchloan.query.message.*;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ClientSessionFactory;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

import javax.annotation.PreDestroy;
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
    private final ClientSession replySession;
    private final ClientSessionFactory replyFactory;

    public ApiHandler(ServerLocator locator, MQConsumerUtils mqConsumerUtils, QueueMessageService queueMessageService) throws Exception {
        this.queueMessageService = queueMessageService;
        ReplyWaitingHandler replyWaitingHandler = new ReplyWaitingHandler();

        replyFactory = locator.createSessionFactory();
        replySession = replyFactory.createSession();
        replySession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        replySession.addMetaData("jms-client-id", "query-reply");
        SimpleString replyQueue = SimpleString.toSimpleString("query-reply-" + UUID.randomUUID());
        mqConsumerUtils.bindConsumer(replySession, replyQueue, true, replyWaitingHandler::handleReplies);
        replySession.start();

        queryServiceRequestProducer = new QueryServiceRequestProducer(mqConsumerUtils, replyQueue);
        queryAccountProducer = new QueryAccountProducer(mqConsumerUtils, replyQueue);
        queryLoanProducer = new QueryLoanProducer(mqConsumerUtils, replyQueue);
        queryStatementProducer = new QueryStatementProducer(mqConsumerUtils, replyQueue);
        queryStatementsProducer = new QueryStatementsProducer(mqConsumerUtils, replyQueue);
        queryCheckRequestProducer = new QueryCheckRequestProducer(mqConsumerUtils, replyQueue);

        queueMessageService.initialize(locator, replyWaitingHandler, "query");
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException, InterruptedException {
        replySession.close();
        replyFactory.close();
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
                                queueMessageService.addMessage(queueMessageHandlerProducer, responseKey, id);
                                return queueMessageService.getReply(responseKey);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                throw new ApiException(e);
                            }
                        }), Object.class);
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
                        queueMessageService.addMessage(queryCheckRequestProducer, responseKey, new byte[0]);
                        return (Boolean) queueMessageService.getReply(responseKey);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new ApiException(e);
                    }
                })
                , Boolean.class);
    }
}
