package com.github.karlnicholas.merchloan.servicerequest.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.karlnicholas.merchloan.apimessage.message.StatementRequest;
import com.github.karlnicholas.merchloan.dto.RequestStatusDto;
import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementCompleteResponse;
import com.github.karlnicholas.merchloan.servicerequest.model.ServiceRequest;
import com.github.karlnicholas.merchloan.servicerequest.service.QueryService;
import com.github.karlnicholas.merchloan.servicerequest.service.ServiceRequestService;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final ServiceRequestService serviceRequestService;
    private final MQConsumerUtils mqConsumerUtils;
    private final ClientSession clientSession;
    private final ClientSessionFactory producerFactory;
    private final ClientProducer serviceRequestQueryIdReplyProducer;
    private final ClientProducer checkRequestReplyProducer;
    private final ClientConsumer servicerequestQueue;
    private final ClientConsumer servicerequestQueryIdQueue;
    private final ClientConsumer serviceRequestCheckRequestQueue;
    private final ClientConsumer serviceRequestBillLoanQueue;
    private final ClientConsumer serviceRequestStatementCompleteQueue;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;

    public MQConsumers(ServerLocator locator, MQConsumerUtils mqConsumerUtils, QueryService queryService, ServiceRequestService serviceRequestService) throws Exception {
        producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "servicerequest-consumers");
        this.mqConsumerUtils = mqConsumerUtils;
        this.serviceRequestService = serviceRequestService;
        this.queryService = queryService;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        servicerequestQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getServicerequestQueue()), false, this::receivedServiceRequestMessage);
        servicerequestQueryIdQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getServicerequestQueryIdQueue()), false, this::receivedServiceRequestQueryIdMessage);
        serviceRequestCheckRequestQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestCheckRequestQueue()), false, this::receivedCheckRequestMessage);
        serviceRequestBillLoanQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestBillLoanQueue()), false, this::receivedServiceRequestBillloanMessage);
        serviceRequestStatementCompleteQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getServiceRequestStatementCompleteQueue()), false, this::receivedServiceStatementCompleteMessage);

        serviceRequestQueryIdReplyProducer = clientSession.createProducer();
        checkRequestReplyProducer = clientSession.createProducer();
        clientSession.start();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("Consumer PreDestroy");
        servicerequestQueue.close();
        servicerequestQueryIdQueue.close();
        serviceRequestCheckRequestQueue.close();
        serviceRequestBillLoanQueue.close();
        serviceRequestStatementCompleteQueue.close();

        clientSession.deleteQueue(mqConsumerUtils.getServicerequestQueue());
        clientSession.deleteQueue(mqConsumerUtils.getServicerequestQueryIdQueue());
        clientSession.deleteQueue(mqConsumerUtils.getServiceRequestCheckRequestQueue());
        clientSession.deleteQueue(mqConsumerUtils.getServiceRequestBillLoanQueue());
        clientSession.deleteQueue(mqConsumerUtils.getServiceRequestStatementCompleteQueue());

        clientSession.close();
        producerFactory.close();
    }

    public void receivedServiceRequestQueryIdMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            UUID id = (UUID) SerializationUtils.deserialize(mo);
            log.debug("ServiceRequestQueryId Received {}", id);
            Optional<ServiceRequest> requestOpt = queryService.getServiceRequest(id);
            String response;
            if (requestOpt.isPresent()) {
                ServiceRequest request = requestOpt.get();
                response = objectMapper.writeValueAsString(RequestStatusDto.builder()
                        .id(request.getId())
                        .localDateTime(request.getLocalDateTime())
                        .status(request.getStatus().name())
                        .statusMessage(request.getStatusMessage())
                        .build());
            } else {
                response = "ERROR: id not found: " + id;
            }
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.writeBodyBufferBytes(SerializationUtils.serialize(response));
            serviceRequestQueryIdReplyProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception e) {
            log.error("receivedCheckRequestMessage", e);
        }
    }


    public void receivedCheckRequestMessage(ClientMessage message) {
        try {
            log.debug("CheckRequest Received");
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.writeBodyBufferBytes(SerializationUtils.serialize(queryService.checkRequest()));
            checkRequestReplyProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception e) {
            log.error("receivedCheckRequestMessage", e);
        }
    }

    public void receivedServiceRequestMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            ServiceRequestResponse serviceRequest = (ServiceRequestResponse) SerializationUtils.deserialize(mo);
            log.debug("ServiceRequestResponse Received {}", serviceRequest);
            serviceRequestService.completeServiceRequest(serviceRequest);
        } catch (Exception ex) {
            log.error("receivedServiceRequestMessage", ex);
        }
    }

    public void receivedServiceRequestBillloanMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            BillingCycle billingCycle = (BillingCycle) SerializationUtils.deserialize(mo);
            if ( billingCycle == null ) {
                throw new IllegalStateException("Message body null");
            }
            log.debug("Billloan Received {}", billingCycle);
            serviceRequestService.statementStatementRequest(StatementRequest.builder()
                            .loanId(billingCycle.getLoanId())
                            .statementDate(billingCycle.getStatementDate())
                            .startDate(billingCycle.getStartDate())
                            .endDate(billingCycle.getEndDate())
                            .build(),
                    Boolean.FALSE, null);
        } catch ( Exception ex) {
            log.error("receivedServiceRequestBillloanMessage", ex);
        }
    }

    public void receivedServiceStatementCompleteMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            StatementCompleteResponse statementCompleteResponse = (StatementCompleteResponse) SerializationUtils.deserialize(mo);
            log.debug("StatementComplete Received {}", statementCompleteResponse);
            serviceRequestService.statementComplete(statementCompleteResponse);
        } catch (Exception ex) {
            log.error("receivedServiceStatementCompleteMessage", ex);
        }
    }

}