package com.github.karlnicholas.merchloan.servicerequest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.karlnicholas.merchloan.apimessage.message.*;
import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.redis.component.RedisComponent;
import com.github.karlnicholas.merchloan.servicerequest.component.ServiceRequestException;
import com.github.karlnicholas.merchloan.servicerequest.dao.ServiceRequestDao;
import com.github.karlnicholas.merchloan.servicerequest.model.ServiceRequest;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class ServiceRequestService {
    private final com.rabbitmq.client.Connection producerConnection;
    private final MQConsumerUtils mqConsumerUtils;
    private final Channel accountCreateAccountProducer;
    private final Channel accountFundingProducer;
    private final Channel accountValidateCreditProducer;
    private final Channel accountValidateDebitProducer;
    private final Channel statementStatementProducer;
    private final Channel accountCloseLoanProducer;
    private final ServiceRequestDao serviceRequestDao;
    private final ObjectMapper objectMapper;
    private final RedisComponent redisComponent;
    private final DataSource dataSource;

    public ServiceRequestService(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils, ServiceRequestDao serviceRequestDao, ObjectMapper objectMapper, RedisComponent redisComponent, DataSource dataSource) throws Exception {
        this.serviceRequestDao = serviceRequestDao;
        this.objectMapper = objectMapper;
        this.redisComponent = redisComponent;
        this.dataSource = dataSource;
        this.mqConsumerUtils = mqConsumerUtils;

        producerConnection = connectionFactory.newConnection();
        accountCreateAccountProducer = producerConnection.createChannel();
        accountFundingProducer = producerConnection.createChannel();
        accountValidateCreditProducer = producerConnection.createChannel();
        accountValidateDebitProducer = producerConnection.createChannel();
        statementStatementProducer = producerConnection.createChannel();
        accountCloseLoanProducer = producerConnection.createChannel();
    }

    @PreDestroy
    public void preDestroy() throws IOException {
        producerConnection.close();
    }

    public UUID accountRequest(ServiceRequestMessage serviceRequestMessage, Boolean retry, UUID existingId) throws ServiceRequestException {
        try {
            AccountRequest accountRequest = (AccountRequest) serviceRequestMessage;
            UUID id = retry == Boolean.TRUE ? existingId : persistRequest(accountRequest);
            CreateAccount message = CreateAccount.builder()
                    .id(id)
                    .customer(accountRequest.getCustomer())
                    .createDate(redisComponent.getBusinessDate())
                    .retry(retry)
                    .build();
            accountCreateAccountProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCreateAccountQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(message));
            return id;
        } catch (SQLException | IOException e) {
            throw new ServiceRequestException(e);
        }
    }

    public UUID fundingRequest(ServiceRequestMessage serviceRequestMessage, Boolean retry, UUID existingId) throws ServiceRequestException {
        try {
            FundingRequest fundingRequest = (FundingRequest) serviceRequestMessage;
            UUID id = retry == Boolean.TRUE ? existingId : persistRequest(fundingRequest);
            FundLoan message = FundLoan.builder()
                    .id(id)
                    .accountId(fundingRequest.getAccountId())
                    .amount(fundingRequest.getAmount())
                    .startDate(redisComponent.getBusinessDate())
                    .description(fundingRequest.getDescription())
                    .retry(retry)
                    .build();
            accountFundingProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountFundingQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(message));
            return id;
        } catch (SQLException | IOException e) {
            throw new ServiceRequestException(e);
        }
    }

    public UUID accountValidateCreditRequest(ServiceRequestMessage serviceRequestMessage, Boolean retry, UUID existingId) throws ServiceRequestException {
        try {
            CreditRequest creditRequest = (CreditRequest) serviceRequestMessage;
            UUID id = retry == Boolean.TRUE ? existingId : persistRequest(creditRequest);
            CreditLoan message = CreditLoan.builder()
                            .id(id)
                            .loanId(creditRequest.getLoanId())
                            .date(redisComponent.getBusinessDate())
                            .amount(creditRequest.getAmount())
                            .description(creditRequest.getDescription())
                            .retry(retry)
                            .build();
            accountValidateCreditProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateCreditQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(message));
            return id;
        } catch (SQLException | IOException e) {
            throw new ServiceRequestException(e);
        }
    }

    public UUID statementStatementRequest(ServiceRequestMessage serviceRequestMessage, Boolean retry, UUID existingId) throws ServiceRequestException {
        try {
            StatementRequest statementRequest = (StatementRequest) serviceRequestMessage;
            UUID id = retry == Boolean.TRUE ? existingId : persistRequest(statementRequest);
            StatementHeaderWork message = StatementHeaderWork.builder().statementHeader(
                            StatementHeader.builder()
                                    .id(id)
                                    .loanId(statementRequest.getLoanId())
                                    .interestChargeId(UUID.randomUUID())
                                    .feeChargeId(UUID.randomUUID())
                                    .statementDate(statementRequest.getStatementDate())
                                    .startDate(statementRequest.getStartDate())
                                    .endDate(statementRequest.getEndDate())
                                    .retry(retry)
                                    .build()
                    ).build();
            statementStatementProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountStatementStatementHeaderQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(message));
            return id;
        } catch (SQLException | IOException e) {
            throw new ServiceRequestException(e);
        }
    }

    public UUID closeRequest(ServiceRequestMessage serviceRequestMessage, Boolean retry, UUID existingId) throws ServiceRequestException {
        try {
            CloseRequest closeRequest = (CloseRequest) serviceRequestMessage;
            UUID id = retry == Boolean.TRUE ? existingId : persistRequest(closeRequest);
            CloseLoan message = CloseLoan.builder()
                            .id(id)
                            .loanId(closeRequest.getLoanId())
                            .interestChargeId(UUID.randomUUID())
                            .paymentId(UUID.randomUUID())
                            .date(redisComponent.getBusinessDate())
                            .amount(closeRequest.getAmount())
                            .description(closeRequest.getDescription())
                            .retry(retry)
                            .build();
            statementStatementProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCloseLoanQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(message));
            return id;
        } catch (SQLException | IOException e) {
            throw new ServiceRequestException(e);
        }
    }

    public UUID accountValidateDebitRequest(ServiceRequestMessage serviceRequestMessage, Boolean retry, UUID existingId) throws ServiceRequestException {
        try {
            DebitRequest debitRequest = (DebitRequest) serviceRequestMessage;
            UUID id = retry == Boolean.TRUE ? existingId : persistRequest(debitRequest);
            DebitLoan message = DebitLoan.builder()
                            .id(id)
                            .loanId(debitRequest.getLoanId())
                            .date(redisComponent.getBusinessDate())
                            .amount(debitRequest.getAmount())
                            .description(debitRequest.getDescription())
                            .retry(retry)
                            .build();
            accountValidateDebitProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateDebitQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(message));
            return id;
        } catch (SQLException | IOException e) {
            throw new ServiceRequestException(e);
        }
    }

    private UUID persistRequest(ServiceRequestMessage requestMessage) throws SQLException, JsonProcessingException {
        UUID id = UUID.randomUUID();
        persistRequestWithId(requestMessage, id);
        return id;
    }

    private void persistRequestWithId(ServiceRequestMessage requestMessage, UUID id) throws JsonProcessingException, SQLException {
        try (Connection con = dataSource.getConnection()) {
            boolean retry;
            do {
                retry = false;
                try {
                    serviceRequestDao.insert(con,
                            ServiceRequest.builder()
                                    .id(id)
                                    .request(objectMapper.writeValueAsString(requestMessage))
                                    .localDateTime(LocalDateTime.now())
                                    .requestType(requestMessage.getClass().getName())
                                    .status(ServiceRequestMessage.STATUS.PENDING)
                                    .retryCount(0)
                                    .build()
                    );
                } catch (DuplicateKeyException dke) {
                    id = UUID.randomUUID();
                    retry = true;
                }
            } while (retry);
        }

    }

    public void completeServiceRequest(ServiceRequestResponse serviceRequestResponse) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            Optional<ServiceRequest> srQ = serviceRequestDao.findById(con, serviceRequestResponse.getId());
            if (srQ.isPresent()) {
                ServiceRequest sr = srQ.get();
                sr.setStatus(serviceRequestResponse.getStatus());
                sr.setStatusMessage(serviceRequestResponse.getStatusMessage());
                serviceRequestDao.update(con, sr);
            } else {
                log.error("void completeServiceRequest(ServiceRequestResponse serviceRequestResponse) not found: {}", serviceRequestResponse);
            }
        }
    }

    public void statementComplete(StatementCompleteResponse statementCompleteResponse) throws SQLException {
        completeServiceRequest(statementCompleteResponse);
    }

}
