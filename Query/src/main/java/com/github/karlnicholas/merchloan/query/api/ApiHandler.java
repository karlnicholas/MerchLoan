package com.github.karlnicholas.merchloan.query.api;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jms.ReplyWaitingHandler;
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
    private final ReplyWaitingHandler replyWaitingHandler;
    private SimpleString replyQueue;
    private final ClientSession replySession;


    public ApiHandler(ServerLocator locator, MQConsumerUtils mqConsumerUtils, QueueMessageService queueMessageService) throws Exception {
        this.queueMessageService = queueMessageService;
        this.replyWaitingHandler = new ReplyWaitingHandler();

        ClientSessionFactory replyFactory =  locator.createSessionFactory();
        replySession = replyFactory.createSession();
        replySession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        replySession.addMetaData("jms-client-id", "query-reply");
        replyQueue = SimpleString.toSimpleString("query-reply-"+UUID.randomUUID());
        mqConsumerUtils.bindConsumer(replySession, replyQueue, true, replyWaitingHandler::handleReplies);
        replySession.start();

        queryServiceRequestProducer = new QueryServiceRequestProducer(mqConsumerUtils, replyWaitingHandler, replyQueue);
        queryAccountProducer = new QueryAccountProducer(mqConsumerUtils, replyWaitingHandler, replyQueue);
        queryLoanProducer = new QueryLoanProducer(mqConsumerUtils, replyWaitingHandler, replyQueue);
        queryStatementProducer = new QueryStatementProducer(mqConsumerUtils, replyWaitingHandler, replyQueue);
        queryStatementsProducer = new QueryStatementsProducer(mqConsumerUtils, replyWaitingHandler, replyQueue);
        queryCheckRequestProducer = new QueryCheckRequestProducer(mqConsumerUtils, replyWaitingHandler, replyQueue);
    }

    public Mono<ServerResponse> getRequest(ServerRequest serverRequest) {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(
                Mono.just(UUID.fromString(serverRequest.pathVariable("id")))
                        .map(id->{
                            try {
                                String responseKey = UUID.randomUUID().toString();
                                queueMessageService.addMessage(queryServiceRequestProducer, responseKey, id);
                                return (String)queueMessageService.getReply(responseKey);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        })
                , String.class);
    }

    public Mono<ServerResponse> getAccount(ServerRequest serverRequest) {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(
                Mono.just(UUID.fromString(serverRequest.pathVariable("id")))
                        .map(id->{
                            try {
                                String responseKey = UUID.randomUUID().toString();
                                queueMessageService.addMessage(queryAccountProducer, responseKey, id);
                                return (String)queueMessageService.getReply(responseKey);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        })
                , String.class);
    }

    public Mono<ServerResponse> getLoan(ServerRequest serverRequest) {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(
                Mono.just(UUID.fromString(serverRequest.pathVariable("id")))
                        .map(id->{
                            try {
                                String responseKey = UUID.randomUUID().toString();
                                queueMessageService.addMessage(queryLoanProducer, responseKey, id);
                                return (String)queueMessageService.getReply(responseKey);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        })
                , String.class);
    }

    public Mono<ServerResponse> getStatement(ServerRequest serverRequest) {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(
                Mono.just(UUID.fromString(serverRequest.pathVariable("id")))
                        .map(id->{
                            try {
                                String responseKey = UUID.randomUUID().toString();
                                queueMessageService.addMessage(queryStatementProducer, responseKey, id);
                                return (String)queueMessageService.getReply(responseKey);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        })
                , String.class);
    }

    public Mono<ServerResponse> getStatements(ServerRequest serverRequest) {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(
                Mono.just(UUID.fromString(serverRequest.pathVariable("id")))
                        .map(id->{
                            try {
                                String responseKey = UUID.randomUUID().toString();
                                queueMessageService.addMessage(queryStatementsProducer, responseKey, id);
                                return (String)queueMessageService.getReply(responseKey);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        })
                , String.class);
    }

    public Mono<ServerResponse> getCheckRequests() {
        return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON).body(
                Mono.fromSupplier(()->{
                        try {
                            String responseKey = UUID.randomUUID().toString();
                            queueMessageService.addMessage(queryCheckRequestProducer, responseKey, null);
                            return (Boolean)queueMessageService.getReply(responseKey);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                })
                , Boolean.class);
    }
}
