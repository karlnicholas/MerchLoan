package com.github.karlnicholas.merchloan.statement.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.karlnicholas.merchloan.apimessage.message.ServiceRequestMessage;
import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.statement.model.Statement;
import com.github.karlnicholas.merchloan.statement.service.QueryService;
import com.github.karlnicholas.merchloan.statement.service.StatementService;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientConsumer;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
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
    private final ClientSession clientSession;
    private final MQConsumerUtils mqConsumerUtils;
    private final QueryService queryService;
    private final ClientProducer responseProducer;
    private final ClientConsumer statementStatementQueue;
    private final ClientConsumer statementCloseStatementQueue;
    private final ClientConsumer statementQueryStatementQueue;
    private final ClientConsumer statementQueryStatementsQueue;
    private final ClientConsumer statementQueryMostRecentStatementQueue;
    private final ObjectMapper objectMapper;
    private final StatementService statementService;
    private final MQProducers mqProducers;
    private final BigDecimal interestRate = new BigDecimal("0.10");
    private final BigDecimal interestMonths = new BigDecimal("12");
    private static final String LOG_STRING = "receivedStatementMessage {}";


    public MQConsumers(ClientSession clientSession, MQConsumerUtils mqConsumerUtils, MQProducers mqProducers, StatementService statementService, QueryService queryService) throws IOException, ActiveMQException {
        this.clientSession = clientSession;
        this.mqConsumerUtils = mqConsumerUtils;
        this.statementService = statementService;
        this.mqProducers = mqProducers;
        this.queryService = queryService;
        objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        statementStatementQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getStatementStatementQueue(), false, false, this::receivedStatementMessage);
        statementCloseStatementQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getStatementCloseStatementQueue(), false, false, this::receivedCloseStatementMessage);
        statementQueryStatementQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getStatementQueryStatementQueue(), false, false, this::receivedQueryStatementMessage);
        statementQueryStatementsQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getStatementQueryStatementsQueue(), false, false, this::receivedQueryStatementsMessage);
        statementQueryMostRecentStatementQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getStatementQueryMostRecentStatementQueue(), false, false, this::receivedQueryMostRecentStatementMessage);

        responseProducer = clientSession.createProducer();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        statementStatementQueue.close();
        statementCloseStatementQueue.close();
        statementQueryStatementQueue.close();
        statementQueryStatementsQueue.close();
        statementQueryMostRecentStatementQueue.close();

        clientSession.deleteQueue(mqConsumerUtils.getStatementStatementQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementCloseStatementQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementQueryStatementQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementQueryStatementsQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementQueryMostRecentStatementQueue());
    }

    public void receivedQueryStatementMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            UUID loanId = (UUID) SerializationUtils.deserialize(mo);
            log.debug("receivedQueryStatementMessage {}", loanId);
            String result = queryService.findById(loanId).map(Statement::getStatementDoc).orElse("ERROR: No statement found for id " + loanId);
            reply(message, result);
        } catch (Exception ex) {
            log.error("receivedQueryStatementMessage exception {}", ex.getMessage());
        }
    }

    public void receivedQueryMostRecentStatementMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            UUID loanId = (UUID) SerializationUtils.deserialize(mo);
            log.debug("receivedQueryMostRecentStatementMessage {}", loanId);
            MostRecentStatement mostRecentStatement = queryService.findMostRecentStatement(loanId).map(statement -> MostRecentStatement.builder()
                            .id(statement.getId())
                            .loanId(loanId)
                            .statementDate(statement.getStatementDate())
                            .endingBalance(statement.getEndingBalance())
                            .startingBalance(statement.getStartingBalance())
                            .build())
                    .orElse(MostRecentStatement.builder().loanId(loanId).build());
            reply(message, mostRecentStatement);
        } catch (Exception ex) {
            log.error("receivedQueryMostRecentStatementMessage exception {}", ex.getMessage());
        }
    }

    public void receivedQueryStatementsMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            UUID id = (UUID) SerializationUtils.deserialize(mo);
            log.debug("receivedQueryStatementsMessage Received {}", id);
            reply(message, objectMapper.writeValueAsString(queryService.findByLoanId(id)));
        } catch (Exception ex) {
            log.error("receivedQueryStatementsMessage exception {}", ex.getMessage());
        }
    }

    private void reply(ClientMessage origMessage, Object data) throws ActiveMQException {
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(origMessage.getCorrelationID());
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(data));
        responseProducer.send(origMessage.getReplyTo(), message);
    }

    public void receivedStatementMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(mo);
        if (statementHeader == null) {
            throw new IllegalStateException("Message body null");
        }
        StatementCompleteResponse requestResponse = StatementCompleteResponse.builder()
                .id(statementHeader.getId())
                .statementDate(statementHeader.getStatementDate())
                .loanId(statementHeader.getLoanId())
                .build();
        boolean loanClosed = false;
        try {
            log.debug(LOG_STRING, statementHeader);
            statementHeader = (StatementHeader) mqProducers.accountQueryStatementHeader(statementHeader);
            if (statementHeader.getCustomer() == null) {
                requestResponse.setFailure("ERROR: Account/Loan not found for accountId " + statementHeader.getAccountId() + " and loanId " + statementHeader.getLoanId());
                return;
            }
            Optional<Statement> statementExistsOpt = statementService.findStatement(statementHeader.getLoanId(), statementHeader.getStatementDate());
            if (statementExistsOpt.isPresent()) {
                requestResponse.setFailure("ERROR: Statement already exists for loanId " + statementHeader.getLoanId() + " and statement date " + statementHeader.getStatementDate());
                return;
            }
            Optional<Statement> lastStatement = statementService.findLastStatement(statementHeader.getLoanId());
            // determine interest balance
            BigDecimal interestBalance = lastStatement.isPresent() ? lastStatement.get().getEndingBalance() : statementHeader.getLoanFunding();
            boolean paymentCreditFound = statementHeader.getRegisterEntries().stream().anyMatch(re -> re.getCredit() != null);
            // so, let's do interest and fee calculations here.
            if (!paymentCreditFound) {
                RegisterEntryMessage feeRegisterEntry = (RegisterEntryMessage) mqProducers.accountBillingCycleCharge(BillingCycleCharge.builder()
                        .id(statementHeader.getFeeChargeId())
                        .loanId(statementHeader.getLoanId())
                        .date(statementHeader.getStatementDate())
                        .debit(new BigDecimal("30.00"))
                        .description("Non payment fee")
                        .retry(statementHeader.getRetry())
                        .build()
                );
                statementHeader.getRegisterEntries().add(feeRegisterEntry);
            }
            BigDecimal interestAmt = interestBalance.multiply(interestRate).divide(interestMonths, 2, RoundingMode.HALF_EVEN);
            RegisterEntryMessage interestRegisterEntry = (RegisterEntryMessage) mqProducers.accountBillingCycleCharge(BillingCycleCharge.builder()
                    .id(statementHeader.getInterestChargeId())
                    .loanId(statementHeader.getLoanId())
                    .date(statementHeader.getStatementDate())
                    .debit(interestAmt)
                    .description("Interest")
                    .retry(statementHeader.getRetry())
                    .build()
            );
            statementHeader.getRegisterEntries().add(interestRegisterEntry);
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
            requestResponse.setSuccess();
            if (endingBalance.compareTo(BigDecimal.ZERO) <= 0) {
                mqProducers.accountLoanClosed(statementHeader);
                loanClosed = true;
            }
        } catch (InterruptedException iex) {
            log.error(LOG_STRING, iex);
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            log.error(LOG_STRING, ex);
            requestResponse.setError(ex.getMessage());
        } finally {
            if (!loanClosed) {
                try {
                    mqProducers.serviceRequestStatementComplete(requestResponse);
                } catch (ActiveMQException innerEx) {
                    log.error("ERROR SENDING ERROR", innerEx);
                }
            }
        }
    }

    public void receivedCloseStatementMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(mo);
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
            mqProducers.accountLoanClosed(statementHeader);
        } catch (Exception ex) {
            log.error("receivedCloseStatementMessage{}", ex.getMessage());
            try {
                ServiceRequestResponse requestResponse = new ServiceRequestResponse(statementHeader.getId(), ServiceRequestMessage.STATUS.ERROR, ex.getMessage());
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (Exception innerEx) {
                log.error("ERROR SENDING ERROR", innerEx);
            }
        }
    }
}