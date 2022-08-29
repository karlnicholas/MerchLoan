package com.github.karlnicholas.merchloan.statement.message;

import com.github.karlnicholas.merchloan.apimessage.message.ServiceRequestMessage;
import com.github.karlnicholas.merchloan.dto.LoanDto;
import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.statement.model.Statement;
import com.github.karlnicholas.merchloan.statement.service.QueryService;
import com.github.karlnicholas.merchloan.statement.service.StatementService;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final Connection consumerConnection;
    private final Connection producerConnection;
    private final MQConsumerUtils mqConsumerUtils;
    private final QueryService queryService;
    private final Channel queryStatementProducer;
    private final Channel queryMostRecentStatementProducer;
    private final Channel queryStatementsProducer;
    private final Channel statementContinueProducer;
    private final Channel statementContinue2Producer;
    private final Channel statementContinue3Producer;
    private final Channel closeStatementProducer;
//    private final ClientConsumer statementLoanIdQueue;
    private final StatementService statementService;
    private final BigDecimal interestRate = new BigDecimal("0.10");
    private final BigDecimal interestMonths = new BigDecimal("12");


    public MQConsumers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils, StatementService statementService, QueryService queryService) throws Exception {
        this.mqConsumerUtils = mqConsumerUtils;
        this.statementService = statementService;
        this.queryService = queryService;

        consumerConnection = connectionFactory.newConnection();
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementContinueQueue(), false, this::receivedStatementContinueMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementContinue2Queue(), false, this::receivedStatementContinue2Message);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementContinue3Queue(), false, this::receivedStatementContinue3Message);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementCloseStatementQueue(), false, this::receivedCloseStatementMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementQueryStatementQueue(), false, this::receivedQueryStatementMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementQueryStatementsQueue(), false, this::receivedQueryStatementsMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementQueryMostRecentStatementQueue(), false, this::receivedQueryMostRecentStatementMessage);

        producerConnection = connectionFactory.newConnection();
        queryStatementProducer = producerConnection.createChannel();
        queryMostRecentStatementProducer = producerConnection.createChannel();
        queryStatementsProducer = producerConnection.createChannel();
        statementContinueProducer = producerConnection.createChannel();
        statementContinue2Producer = producerConnection.createChannel();
        statementContinue3Producer = producerConnection.createChannel();
        closeStatementProducer = producerConnection.createChannel();
    }

    @PreDestroy
    public void preDestroy() throws IOException {
        consumerConnection.close();
        producerConnection.close();
    }

    public void receivedQueryStatementMessage(String consumerTag, Delivery message) {
        try {
            UUID loanId = (UUID) SerializationUtils.deserialize(message.getBody());
            log.debug("receivedQueryStatementMessage {}", loanId);
            String result = queryService.findById(loanId).map(Statement::getStatementDoc).orElse("ERROR: No statement found for id " + loanId);
            queryStatementProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize(result));
        } catch (Exception ex) {
            log.error("receivedQueryStatementMessage exception", ex);
        }
    }

    public void receivedQueryMostRecentStatementMessage(String consumerTag, Delivery message) {
        log.debug("receivedQueryMostRecentStatementMessage PRE");
        try {
            LoanDto loanDto = (LoanDto) SerializationUtils.deserialize(message.getBody());
            log.debug("receivedQueryMostRecentStatementMessage {}", loanDto);
            queryService.findMostRecentStatement(loanDto.getLoanId()).ifPresent(statement -> {
                loanDto.setLastStatementDate(statement.getStatementDate());
                loanDto.setLastStatementBalance(statement.getEndingBalance());

            });
            queryMostRecentStatementProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountLoanIdComputeQueue(), message.getProperties(), SerializationUtils.serialize(loanDto));
        } catch (Exception ex) {
            log.error("receivedQueryMostRecentStatementMessage exception", ex);
        }
    }

    public void receivedQueryStatementsMessage(String consumerTag, Delivery message) {
        try {
            UUID id = (UUID) SerializationUtils.deserialize(message.getBody());
            log.debug("receivedQueryStatementsMessage Received {}", id);
            queryStatementsProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize(queryService.findByLoanId(id)));
        } catch (Exception ex) {
            log.error("receivedQueryStatementsMessage exception", ex);
        }
    }

    public void receivedStatementContinueMessage(String consumerTag, Delivery message) {
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(message.getBody());
        try {
            log.debug("receivedStatementContinueMessage: getStatementHeader: {}", statementHeaderWork.getStatementHeader());
            Optional<Statement> statementExistsOpt = statementService.findStatement(
                    statementHeaderWork.getStatementHeader().getLoanId(),
                    statementHeaderWork.getStatementHeader().getStatementDate()
            );
            if (statementExistsOpt.isPresent()) {
                ServiceRequestResponse requestResponse =
                        new ServiceRequestResponse(statementHeaderWork.getStatementHeader().getId(), ServiceRequestMessage.STATUS.ERROR,
                            "ERROR: Statement already exists for loanId "
                                        + statementHeaderWork.getStatementHeader().getLoanId()
                                        + " and statement date "
                                        + statementHeaderWork.getStatementHeader().getStatementDate()
                        );
                serviceRequestServiceRequest(requestResponse, statementContinueProducer);
                return;
            }
            // determine interest balance
            boolean paymentCreditFound = statementHeaderWork.getStatementHeader().getRegisterEntries().stream().anyMatch(re -> re.getCredit() != null);
            // so, let's do interest and fee calculations here.
            if (!paymentCreditFound) {
                BillingCycleCharge feeCharge = BillingCycleCharge.builder()
                        .id(statementHeaderWork.getStatementHeader().getFeeChargeId())
                        .loanId(statementHeaderWork.getStatementHeader().getLoanId())
                        .date(statementHeaderWork.getStatementHeader().getStatementDate())
                        .debit(new BigDecimal("30.00"))
                        .description("Non payment fee")
                        .retry(statementHeaderWork.getStatementHeader().getRetry())
                        .build();
                log.debug("receivedStatementContinueMessage: {}", feeCharge);
                statementHeaderWork.setBillingCycleCharge(feeCharge);
                statementContinueProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountFeeChargeQueue(), message.getProperties(), SerializationUtils.serialize(statementHeaderWork));
            } else {
                statementContinueProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementContinue2Queue(), message.getProperties(), SerializationUtils.serialize(statementHeaderWork));
            }
        } catch (Exception ex) {
            log.error("receivedStatementContinueMessage", ex);
            try {
                ServiceRequestResponse requestResponse = new ServiceRequestResponse(statementHeaderWork.getStatementHeader().getId(), ServiceRequestMessage.STATUS.ERROR, ex.getMessage());
                serviceRequestServiceRequest(requestResponse, statementContinueProducer);
            } catch (Exception innerEx) {
                log.error("ERROR SENDING ERROR", innerEx);
            }
        }
    }

    public void receivedStatementContinue2Message(String consumerTag, Delivery message) {
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(message.getBody());
        try {
            log.debug("receivedStatementContinue2Message: getStatementHeader: {}", statementHeaderWork.getStatementHeader());
            Optional<Statement> lastStatement = statementService.findLastStatement(statementHeaderWork.getStatementHeader().getLoanId());
            BigDecimal interestBalance;
            if (lastStatement.isPresent()) {
                interestBalance = lastStatement.get().getEndingBalance();
                statementHeaderWork.setLastStatementPresent(Boolean.TRUE);
                statementHeaderWork.setLastStatementEndingBalance(interestBalance);
            } else {
                statementHeaderWork.setLastStatementPresent(lastStatement.isPresent());
                interestBalance = statementHeaderWork.getStatementHeader().getLoanFunding();
                statementHeaderWork.setLastStatementPresent(Boolean.FALSE);
            }
            BigDecimal interestAmt = interestBalance.multiply(interestRate).divide(interestMonths, 2, RoundingMode.HALF_EVEN);
            log.debug("receivedStatementMessage: interestAmt: {}", interestAmt);
            BillingCycleCharge interestCharge = BillingCycleCharge.builder()
                    .id(statementHeaderWork.getStatementHeader().getInterestChargeId())
                    .loanId(statementHeaderWork.getStatementHeader().getLoanId())
                    .date(statementHeaderWork.getStatementHeader().getStatementDate())
                    .debit(interestAmt)
                    .description("Interest")
                    .retry(statementHeaderWork.getStatementHeader().getRetry())
                    .build();
            statementHeaderWork.setBillingCycleCharge(interestCharge);
            statementContinue2Producer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountInterestChargeQueue(), message.getProperties(), SerializationUtils.serialize(statementHeaderWork));
        } catch (Exception ex) {
            log.error("receivedStatementContinueMessage", ex);
            try {
                ServiceRequestResponse requestResponse = new ServiceRequestResponse(statementHeaderWork.getStatementHeader().getId(), ServiceRequestMessage.STATUS.ERROR, ex.getMessage());
                serviceRequestServiceRequest(requestResponse, statementContinue2Producer);
            } catch (Exception innerEx) {
                log.error("ERROR SENDING ERROR", innerEx);
            }
        }
    }

    public void receivedStatementContinue3Message(String consumerTag, Delivery message) {
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(message.getBody());
        try {
            log.debug("receivedStatementContinue3Message: getStatementHeader: {}", statementHeaderWork.getStatementHeader());
            BigDecimal startingBalance = statementHeaderWork.getLastStatementPresent() ? statementHeaderWork.getLastStatementEndingBalance() : BigDecimal.valueOf(0, 2);
            BigDecimal endingBalance = startingBalance;
            for (RegisterEntryMessage re : statementHeaderWork.getStatementHeader().getRegisterEntries()) {
                if (re.getCredit() != null) {
                    endingBalance = endingBalance.subtract(re.getCredit());
                    re.setBalance(endingBalance);
                }
                if (re.getDebit() != null) {
                    endingBalance = endingBalance.add(re.getDebit());
                    re.setBalance(endingBalance);
                }
            }
            // so, done with interest and fee calculations here?
            statementService.saveStatement(statementHeaderWork.getStatementHeader(), startingBalance, endingBalance);
            ServiceRequestResponse requestResponse = ServiceRequestResponse.builder().id(statementHeaderWork.getStatementHeader().getId()).build();
            requestResponse.setSuccess();
            log.debug("receivedStatementContinue3Message: requestResponse: {}", requestResponse);
            if (endingBalance.compareTo(BigDecimal.ZERO) <= 0) {
                sendAccountLoanClosed(statementHeaderWork.getStatementHeader(), statementContinue3Producer);
                return;
            }
            serviceRequestServiceRequest(requestResponse, statementContinue3Producer);
        } catch (Exception ex) {
            log.error("receivedStatementContinueMessage", ex);
            try {
                ServiceRequestResponse requestResponse = new ServiceRequestResponse(statementHeaderWork.getStatementHeader().getId(), ServiceRequestMessage.STATUS.ERROR, ex.getMessage());
                serviceRequestServiceRequest(requestResponse, statementContinue3Producer);
            } catch (Exception innerEx) {
                log.error("ERROR SENDING ERROR", innerEx);
            }
        }
    }

    private void sendAccountLoanClosed(StatementHeader statementHeader, Channel producer) throws IOException {
        producer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountLoanClosedQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(statementHeader));
    }

    public void receivedCloseStatementMessage(String consumerTag, Delivery message) {
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(message.getBody());
        try {
            log.debug("receivedCloseStatementMessage {}", statementHeader);
            Optional<Statement> statementExistsOpt = statementService.findStatement(statementHeader.getLoanId(), statementHeader.getStatementDate());
            if (statementExistsOpt.isEmpty()) {
                // determine interest balance
                Optional<Statement> lastStatement = statementService.findLastStatement(statementHeader.getLoanId());
                BigDecimal startingBalance = lastStatement.isPresent() ? lastStatement.get().getEndingBalance() : BigDecimal.ZERO.setScale(2, RoundingMode.HALF_EVEN);
                BigDecimal endingBalance = startingBalance;
                for (RegisterEntryMessage re : statementHeader.getRegisterEntries()) {
                    if (re.getCredit() != null) {
                        endingBalance = endingBalance.subtract(re.getCredit());
                        re.setBalance(endingBalance);
                    }
                    if (re.getDebit() != null) {
                        endingBalance = endingBalance.add(re.getDebit());
                        re.setBalance(endingBalance);
                    }
                }
                // so, done with interest and fee calculations here?
                statementService.saveStatement(statementHeader, startingBalance, endingBalance);
            }
            // just to save bandwidth
            statementHeader.setRegisterEntries(null);
            sendAccountLoanClosed(statementHeader, closeStatementProducer);
        } catch (Exception ex) {
            log.error("receivedCloseStatementMessage", ex);
            try {
                ServiceRequestResponse requestResponse = new ServiceRequestResponse(statementHeader.getId(), ServiceRequestMessage.STATUS.ERROR, ex.getMessage());
                serviceRequestServiceRequest(requestResponse, closeStatementProducer);
            } catch (Exception innerEx) {
                log.error("ERROR SENDING ERROR", innerEx);
            }
        }
    }
    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest, Channel producer) throws IOException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        producer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), new AMQP.BasicProperties.Builder().build(), SerializationUtils.serialize(serviceRequest));
    }

}