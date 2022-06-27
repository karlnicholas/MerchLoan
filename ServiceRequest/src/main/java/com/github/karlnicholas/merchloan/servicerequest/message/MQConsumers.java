package com.github.karlnicholas.merchloan.servicerequest.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.karlnicholas.merchloan.apimessage.message.StatementRequest;
import com.github.karlnicholas.merchloan.dto.RequestStatusDto;
import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.BillingCycle;
import com.github.karlnicholas.merchloan.jmsmessage.ServiceRequestResponse;
import com.github.karlnicholas.merchloan.jmsmessage.StatementCompleteResponse;
import com.github.karlnicholas.merchloan.servicerequest.component.ServiceRequestException;
import com.github.karlnicholas.merchloan.servicerequest.model.ServiceRequest;
import com.github.karlnicholas.merchloan.servicerequest.service.QueryService;
import com.github.karlnicholas.merchloan.servicerequest.service.ServiceRequestService;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final ServiceRequestService serviceRequestService;
    private final ClientSession clientSession;
    private final ClientProducer responseProducer;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;

    public MQConsumers(ClientSession clientSession, MQConsumerUtils mqConsumerUtils, QueryService queryService, ServiceRequestService serviceRequestService) throws IOException, ActiveMQException {
        this.clientSession = clientSession;
        this.serviceRequestService = serviceRequestService;
        this.queryService = queryService;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getServicerequestQueue(), true, this::receivedServiceRequestMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getServicerequestQueryIdQueue(), true, this::receivedServiceRequestQueryIdMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getServiceRequestCheckRequestQueue(), true, this::receivedCheckRequestMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getServiceRequestBillLoanQueue(), true, this::receivedServiceRequestBillloanMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getServiceRequestStatementCompleteQueue(), true, this::receivedServiceStatementCompleteMessage);

        responseProducer = clientSession.createProducer();
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
            reply(message, response);
        } catch (Exception e) {
            log.error("receivedCheckRequestMessage", e);
        }
    }


    public void receivedCheckRequestMessage(ClientMessage message) {
        log.debug("CheckRequest Received");
        try {
            reply(message, queryService.checkRequest());
        } catch (Exception e) {
            log.error("receivedCheckRequestMessage", e);
        }
    }

    private void reply(ClientMessage origMessage, Object data) throws ActiveMQException {
        ClientMessage message = clientSession.createMessage(false);
        byte[] mo = SerializationUtils.serialize(data);
        message.writeBodyBufferBytes(mo);
        message.setCorrelationID(origMessage.getCorrelationID());
        responseProducer.send(origMessage.getReplyTo(), message);
    }

    public void receivedServiceRequestMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        ServiceRequestResponse serviceRequest = (ServiceRequestResponse) SerializationUtils.deserialize(mo);
        log.debug("ServiceRequestResponse Received {}", serviceRequest);
        try {
            serviceRequestService.completeServiceRequest(serviceRequest);
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void receivedServiceRequestBillloanMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        BillingCycle billingCycle = (BillingCycle) SerializationUtils.deserialize(mo);
        if ( billingCycle == null ) {
            throw new IllegalStateException("Message body null");
        }
        log.debug("Billloan Received {}", billingCycle);
        try {
            serviceRequestService.statementStatementRequest(StatementRequest.builder()
                            .loanId(billingCycle.getLoanId())
                            .statementDate(billingCycle.getStatementDate())
                            .startDate(billingCycle.getStartDate())
                            .endDate(billingCycle.getEndDate())
                            .build(),
                    Boolean.FALSE, null);
        } catch ( ServiceRequestException ex) {
            throw new IllegalStateException(ex);
        }
    }

    public void receivedServiceStatementCompleteMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementCompleteResponse statementCompleteResponse = (StatementCompleteResponse) SerializationUtils.deserialize(mo);
        log.debug("StatementComplete Received {}", statementCompleteResponse);
        try {
            serviceRequestService.statementComplete(statementCompleteResponse);
        } catch (SQLException ex) {
            throw new IllegalStateException(ex);
        }
    }

}