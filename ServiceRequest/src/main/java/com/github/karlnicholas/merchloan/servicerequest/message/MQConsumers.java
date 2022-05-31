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
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.springframework.stereotype.Component;

import jakarta.jms.*;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final ConnectionFactory connectionFactory;
    private final ServiceRequestService serviceRequestService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;

    public MQConsumers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils, QueryService queryService, ServiceRequestService serviceRequestService) throws JMSException {
        this.connectionFactory = connectionFactory;
        this.serviceRequestService = serviceRequestService;
        this.queryService = queryService;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        connectionFactory.createContext().createConsumer(ActiveMQDestination.createDestination(mqConsumerUtils.getServicerequestQueue(), ActiveMQDestination.TYPE.QUEUE)).setMessageListener(this::receivedServiceRequestMessage);
        connectionFactory.createContext().createConsumer(ActiveMQDestination.createDestination(mqConsumerUtils.getServicerequestQueryIdQueue(), ActiveMQDestination.TYPE.QUEUE)).setMessageListener(this::receivedServiceRequestQueryIdMessage);
        connectionFactory.createContext().createConsumer(ActiveMQDestination.createDestination(mqConsumerUtils.getServiceRequestCheckRequestQueue(), ActiveMQDestination.TYPE.QUEUE)).setMessageListener(this::receivedCheckRequestMessage);
        connectionFactory.createContext().createConsumer(ActiveMQDestination.createDestination(mqConsumerUtils.getServiceRequestBillLoanQueue(), ActiveMQDestination.TYPE.QUEUE)).setMessageListener(this::receivedServiceRequestBillloanMessage);
        connectionFactory.createContext().createConsumer(ActiveMQDestination.createDestination(mqConsumerUtils.getServiceRequestStatementCompleteQueue(), ActiveMQDestination.TYPE.QUEUE)).setMessageListener(this::receivedServiceStatementCompleteMessage);

    }

    public void receivedServiceRequestQueryIdMessage(Message message) {
        try {
            UUID id = (UUID) ((ObjectMessage) message).getObject();
            log.debug("receivedServiceRequestQueryIdMessage: {}", id);
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


    public void receivedCheckRequestMessage(Message message) {
        log.trace("receivedCheckRequestMessage");
        try {
            reply(message, queryService.checkRequest());
        } catch (Exception e) {
            log.error("receivedCheckRequestMessage", e);
        }
    }

    public void reply(Message consumerMessage, Serializable data) throws JMSException {
        try ( JMSContext jmsContext = connectionFactory.createContext()) {
            Message message = jmsContext.createObjectMessage(data);
            message.setJMSCorrelationID(consumerMessage.getJMSCorrelationID());
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(consumerMessage.getJMSReplyTo(), message);
        }
    }

    public void receivedServiceRequestMessage(Message message) {
        try {
            ServiceRequestResponse serviceRequest = (ServiceRequestResponse) ((ObjectMessage) message).getObject();
            log.debug("receivedServiceRequestMessage {}", serviceRequest);
            serviceRequestService.completeServiceRequest(serviceRequest);
        } catch (SQLException | JMSException ex) {
            log.error("receivedServiceStatementCompleteMessage", ex);
        }
    }

    public void receivedServiceRequestBillloanMessage(Message message) {
        try {
            BillingCycle billingCycle = (BillingCycle) ((ObjectMessage) message).getObject();
            log.debug("receivedServiceRequestBillloanMessage: {}", billingCycle);
            serviceRequestService.statementStatementRequest(StatementRequest.builder()
                            .loanId(billingCycle.getLoanId())
                            .statementDate(billingCycle.getStatementDate())
                            .startDate(billingCycle.getStartDate())
                            .endDate(billingCycle.getEndDate())
                            .build(),
                    Boolean.FALSE, null);
        } catch (ServiceRequestException | JMSException ex) {
            log.error("receivedServiceStatementCompleteMessage", ex);
        }
    }

    public void receivedServiceStatementCompleteMessage(Message message) {
        try {
            StatementCompleteResponse statementCompleteResponse = (StatementCompleteResponse) ((ObjectMessage) message).getObject();
            log.debug("receivedServiceStatementCompleteMessage: {}", statementCompleteResponse);
            serviceRequestService.statementComplete(statementCompleteResponse);
        } catch (SQLException | JMSException ex) {
            log.error("receivedServiceStatementCompleteMessage", ex);
        }
    }

}