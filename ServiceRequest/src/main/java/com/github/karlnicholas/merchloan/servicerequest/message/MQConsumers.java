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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final ServiceRequestService serviceRequestService;
    private final MQConsumerUtils mqConsumerUtils;
    private final Connection consumerConnection;
    private final Connection producerConnection;
    private final Channel serviceRequestQueryIdReplyProducer;
    private final Channel checkRequestReplyProducer;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;

    public MQConsumers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils, QueryService queryService, ServiceRequestService serviceRequestService) throws Exception {
        consumerConnection = connectionFactory.newConnection();
        this.mqConsumerUtils = mqConsumerUtils;
        this.serviceRequestService = serviceRequestService;
        this.queryService = queryService;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), false, this::receivedServiceRequestMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueryIdQueue(), false, this::receivedServiceRequestQueryIdMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getServiceRequestCheckRequestQueue(), false, this::receivedCheckRequestMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getServiceRequestBillLoanQueue(), false, this::receivedServiceRequestBillloanMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getServiceRequestStatementCompleteQueue(), false, this::receivedServiceStatementCompleteMessage);

        producerConnection = connectionFactory.newConnection();
        serviceRequestQueryIdReplyProducer = producerConnection.createChannel();
        checkRequestReplyProducer = producerConnection.createChannel();
    }

    @PreDestroy
    public void preDestroy() throws IOException {
        log.info("Consumer PreDestroy");

        consumerConnection.close();
        producerConnection.close();
    }

    public void receivedServiceRequestQueryIdMessage(String consumerTag, Delivery message) {
        try {
            UUID id = (UUID) SerializationUtils.deserialize(message.getBody());
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
            serviceRequestQueryIdReplyProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize(response));
        } catch (Exception e) {
            log.error("receivedCheckRequestMessage", e);
        }
    }


    public void receivedCheckRequestMessage(String consumerTag, Delivery message) {
        try {
            log.debug("CheckRequest Received");
            checkRequestReplyProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize(queryService.checkRequest()));
        } catch (Exception e) {
            log.error("receivedCheckRequestMessage", e);
        }
    }

    public void receivedServiceRequestMessage(String consumerTag, Delivery message) {
        try {
            ServiceRequestResponse serviceRequest = (ServiceRequestResponse) SerializationUtils.deserialize(message.getBody());
            log.debug("ServiceRequestResponse Received {}", serviceRequest);
            serviceRequestService.completeServiceRequest(serviceRequest);
        } catch (Exception ex) {
            log.error("receivedServiceRequestMessage", ex);
        }
    }

    public void receivedServiceRequestBillloanMessage(String consumerTag, Delivery message) {
        try {
            BillingCycle billingCycle = (BillingCycle) SerializationUtils.deserialize(message.getBody());
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

    public void receivedServiceStatementCompleteMessage(String consumerTag, Delivery message) {
        try {
            StatementCompleteResponse statementCompleteResponse = (StatementCompleteResponse) SerializationUtils.deserialize(message.getBody());
            log.debug("StatementComplete Received {}", statementCompleteResponse);
            serviceRequestService.statementComplete(statementCompleteResponse);
        } catch (Exception ex) {
            log.error("receivedServiceStatementCompleteMessage", ex);
        }
    }

}