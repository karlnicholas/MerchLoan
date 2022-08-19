package com.github.karlnicholas.merchloan.statement.message;

import com.github.karlnicholas.merchloan.apimessage.message.ServiceRequestMessage;
import com.github.karlnicholas.merchloan.dto.LoanDto;
import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.statement.model.Statement;
import com.github.karlnicholas.merchloan.statement.service.QueryService;
import com.github.karlnicholas.merchloan.statement.service.StatementService;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final ClientSession clientSession;
    private final ClientSessionFactory producerFactory;
    private final MQConsumerUtils mqConsumerUtils;
    private final QueryService queryService;
    private final ClientProducer queryStatementProducer;
    private final ClientProducer queryMostRecentStatementProducer;
    private final ClientProducer queryStatementsProducer;
    private final ClientProducer statementContinueProducer;
    private final ClientProducer statementContinue2Producer;
    private final ClientProducer statementContinue3Producer;
    private final ClientProducer closeStatementProducer;
    //    private final ClientConsumer statementStatementQueue;
    private final ClientConsumer statementContinueQueue;
    private final ClientConsumer statementContinue2Queue;
    private final ClientConsumer statementContinue3Queue;
    private final ClientConsumer statementCloseStatementQueue;
    private final ClientConsumer statementQueryStatementQueue;
    private final ClientConsumer statementQueryStatementsQueue;
    private final ClientConsumer statementQueryMostRecentStatementQueue;
    private final ClientConsumer statementLoanIdQueue;
    private final StatementService statementService;
    private final BigDecimal interestRate = new BigDecimal("0.10");
    private final BigDecimal interestMonths = new BigDecimal("12");


    public MQConsumers(ServerLocator locator, MQConsumerUtils mqConsumerUtils, StatementService statementService, QueryService queryService) throws Exception {
        producerFactory = locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "statement-consumers");
        this.mqConsumerUtils = mqConsumerUtils;
        this.statementService = statementService;
        this.queryService = queryService;
//        objectMapper = new ObjectMapper().findAndRegisterModules()
//                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

//        statementStatementQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementStatementQueue()), false, this::receivedStatementStatementMessage);
        statementContinueQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementContinueQueue()), false, this::receivedStatementContinueMessage);
        statementContinue2Queue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementContinue2Queue()), false, this::receivedStatementContinue2Message);
        statementContinue3Queue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementContinue3Queue()), false, this::receivedStatementContinue3Message);
        statementCloseStatementQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementCloseStatementQueue()), false, this::receivedCloseStatementMessage);
        statementQueryStatementQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementQueryStatementQueue()), false, this::receivedQueryStatementMessage);
        statementQueryStatementsQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementQueryStatementsQueue()), false, this::receivedQueryStatementsMessage);
        statementQueryMostRecentStatementQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementQueryMostRecentStatementQueue()), false, this::receivedQueryMostRecentStatementMessage);
        statementLoanIdQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getStatementLoanIdQueue()), false, this::receivedQueryMostRecentStatementMessage);

        queryStatementProducer = clientSession.createProducer();
        queryMostRecentStatementProducer = clientSession.createProducer();
        queryStatementsProducer = clientSession.createProducer();
        statementContinueProducer = clientSession.createProducer();
        statementContinue2Producer = clientSession.createProducer();
        statementContinue3Producer = clientSession.createProducer();
        closeStatementProducer = clientSession.createProducer();
        clientSession.start();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException {
//        statementStatementQueue.close();
        statementCloseStatementQueue.close();
        statementQueryStatementQueue.close();
        statementQueryStatementsQueue.close();
        statementQueryMostRecentStatementQueue.close();
        statementLoanIdQueue.close();
        statementContinueQueue.close();
        statementContinue2Queue.close();
        statementContinue3Queue.close();

//        clientSession.deleteQueue(mqConsumerUtils.getStatementStatementQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementCloseStatementQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementQueryStatementQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementQueryStatementsQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementQueryMostRecentStatementQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementLoanIdQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementContinueQueue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementContinue2Queue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementContinue3Queue());
        clientSession.deleteQueue(mqConsumerUtils.getStatementLoanIdQueue());

        clientSession.close();
        producerFactory.close();
    }

    public void receivedQueryStatementMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            UUID loanId = (UUID) SerializationUtils.deserialize(mo);
            log.debug("receivedQueryStatementMessage {}", loanId);
            String result = queryService.findById(loanId).map(Statement::getStatementDoc).orElse("ERROR: No statement found for id " + loanId);
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(result));
            replyMessage.setCorrelationID(message.getCorrelationID());
            queryStatementProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedQueryStatementMessage exception", ex);
        }
    }

    public void receivedQueryMostRecentStatementMessage(ClientMessage message) {
        log.debug("receivedQueryMostRecentStatementMessage PRE");
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            LoanDto loanDto = (LoanDto) SerializationUtils.deserialize(mo);
            log.debug("receivedQueryMostRecentStatementMessage {}", loanDto);
            queryService.findMostRecentStatement(loanDto.getLoanId()).ifPresent(statement -> {
                loanDto.setLastStatementDate(statement.getStatementDate());
                loanDto.setLastStatementBalance(statement.getEndingBalance());

            });
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(loanDto));
            replyMessage.setReplyTo(message.getReplyTo());
            replyMessage.setCorrelationID(message.getCorrelationID());
            queryMostRecentStatementProducer.send(mqConsumerUtils.getAccountLoanIdComputeQueue(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedQueryMostRecentStatementMessage exception", ex);
        }
    }

    public void receivedQueryStatementsMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            UUID id = (UUID) SerializationUtils.deserialize(mo);
            log.debug("receivedQueryStatementsMessage Received {}", id);
//            reply(message, objectMapper.writeValueAsString(queryService.findByLoanId(id)));
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(queryService.findByLoanId(id)));
            replyMessage.setCorrelationID(message.getCorrelationID());
            queryStatementsProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedQueryStatementsMessage exception", ex);
        }
    }

//    public void receivedStatementStatementMessage(ClientMessage message) {
//        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
//        message.getBodyBuffer().readBytes(mo);
//        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(mo);
//        if (statementHeader == null) {
//            throw new IllegalStateException("Message body null");
//        }
//        StatementCompleteResponse requestResponse = StatementCompleteResponse.builder()
//                .id(statementHeader.getId())
//                .statementDate(statementHeader.getStatementDate())
//                .loanId(statementHeader.getLoanId())
//                .build();
//        boolean loanClosed = false;
//        try {
//            log.debug("receivedStatementMessage: loanId: {}", statementHeader.getLoanId());
//            statementHeader = (StatementHeader) mqProducers.accountQueryStatementHeader(statementHeader);
//            log.debug("receivedStatementMessage: statementHeader: {}", statementHeader);
//        } catch (InterruptedException iex) {
//            log.error("receivedStatementMessage", iex);
//            Thread.currentThread().interrupt();
//        } catch (Exception ex) {
//            log.error("receivedStatementMessage", ex);
//            requestResponse.setError(ex.getMessage());
//        } finally {
//            if (!loanClosed) {
//                try {
//                    mqProducers.serviceRequestStatementComplete(requestResponse);
//                } catch (ActiveMQException innerEx) {
//                    log.error("ERROR SENDING ERROR", innerEx);
//                }
//            }
//        }
//    }

    public void receivedStatementContinueMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(mo);
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
                ClientMessage feeMessage = clientSession.createMessage(false);
                feeMessage.setReplyTo(message.getReplyTo());
                feeMessage.setCorrelationID(message.getCorrelationID());
                feeMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeaderWork));
                statementContinueProducer.send(mqConsumerUtils.getAccountFeeChargeQueue(), feeMessage);
//                statementHeader.getRegisterEntries().add(feeRegisterEntry);
            } else {
                ClientMessage feeMessage = clientSession.createMessage(false);
                feeMessage.setReplyTo(message.getReplyTo());
                message.setCorrelationID(message.getCorrelationID());
                message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeaderWork));
                statementContinueProducer.send(mqConsumerUtils.getStatementContinue2Queue(), message);
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

    public void receivedStatementContinue2Message(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(mo);
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
            ClientMessage interestMessage = clientSession.createMessage(false);
            interestMessage.setReplyTo(message.getReplyTo());
            interestMessage.setCorrelationID(message.getCorrelationID());
            interestMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeaderWork));
            statementContinue2Producer.send(mqConsumerUtils.getAccountInterestChargeQueue(), interestMessage);
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

    public void receivedStatementContinue3Message(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(mo);
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

    private void sendAccountLoanClosed(StatementHeader statementHeader, ClientProducer producer) throws ActiveMQException {
        log.debug("accountLoanClosed: {}", statementHeader);
        ClientMessage loanClosedMessage = clientSession.createMessage(false);
        loanClosedMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        producer.send(mqConsumerUtils.getAccountLoanClosedQueue(), loanClosedMessage);
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
    public void serviceRequestServiceRequest(ServiceRequestResponse serviceRequest, ClientProducer producer) throws ActiveMQException {
        log.debug("serviceRequestServiceRequest: {}", serviceRequest);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
        producer.send(mqConsumerUtils.getServicerequestQueue(), message);
    }

//    private void receiveStatementLoadIdMessage(ClientMessage message) throws SQLException, ActiveMQException, InterruptedException {
//        // get most recent statement
//        MostRecentStatement mostRecentStatement = (MostRecentStatement) mqProducers.queryMostRecentStatement(loanDto.getLoanId());
//        // generate a simulated new statement for current period
//        StatementHeader statementHeader = StatementHeader.builder().build();
//        statementHeader.setLoanId(loanDto.getLoanId());
//        List<RegisterEntry> registerEntries;
//        if (mostRecentStatement.getStatementDate() == null) {
//            statementHeader.setEndDate(loanDto.getStatementDates().get(0));
//            statementHeader.setStartDate(loanDto.getStartDate());
//            registerEntries = registerEntryDao.findByLoanIdAndDateBetweenOrderByTimestamp(con, statementHeader.getLoanId(), statementHeader.getStartDate(), statementHeader.getEndDate());
//        } else {
//            int index = loanDto.getStatementDates().indexOf(mostRecentStatement.getStatementDate());
//            if (index + 1 < loanDto.getStatementDates().size()) {
//                statementHeader.setEndDate(loanDto.getStatementDates().get(index + 1));
//                statementHeader.setStartDate(loanDto.getStatementDates().get(index).plusDays(1));
//                registerEntries = registerEntryDao.findByLoanIdAndDateBetweenOrderByTimestamp(con, statementHeader.getLoanId(), statementHeader.getStartDate(), statementHeader.getEndDate());
//            } else {
//                registerEntries = new ArrayList<>();
//            }
//        }
//        // determine current balance, payoff amount
//        BigDecimal startingBalance;
//        BigDecimal interestBalance;
//        if (mostRecentStatement.getStatementDate() == null) {
//            startingBalance = BigDecimal.ZERO.setScale(2, RoundingMode.HALF_EVEN);
//            interestBalance = loanDto.getFunding();
//        } else {
//            startingBalance = mostRecentStatement.getEndingBalance();
//            interestBalance = mostRecentStatement.getEndingBalance();
//        }
//        BigDecimal currentBalance = startingBalance;
//        // determine current payoff amount
//        BigDecimal currentInterest = interestBalance.multiply(loanDto.getInterestRate()).divide(BigDecimal.valueOf(loanDto.getMonths()), RoundingMode.HALF_EVEN);
//        for (RegisterEntry re : registerEntries) {
//            if (re.getDebit() != null) {
//                currentBalance = currentBalance.add(re.getDebit());
//            } else if (re.getCredit() != null) {
//                currentBalance = currentBalance.subtract(re.getCredit());
//            }
//        }
//        BigDecimal payoffAmount = currentInterest.add(currentBalance).setScale(2, RoundingMode.HALF_EVEN);
//        // compute current Payment
//        // must first compute expected balance
//        // what is number of months?
//        int nMonths = loanDto.getStatementDates().indexOf(statementHeader.getEndDate()) + 1;
//        BigDecimal computeAmount = loanDto.getFunding();
//        for (int i = 0; i < nMonths; ++i) {
//            BigDecimal computeInterest = computeAmount.multiply(loanDto.getInterestRate()).divide(BigDecimal.valueOf(loanDto.getMonths()), RoundingMode.HALF_EVEN);
//            computeAmount = computeAmount.add(computeInterest).subtract(loanDto.getMonthlyPayments()).setScale(2, RoundingMode.HALF_EVEN);
//        }
//        // fill out additional response
//        BigDecimal currentPayment = currentBalance.add(currentInterest).setScale(2, RoundingMode.HALF_EVEN).subtract(computeAmount);
//        loanDto.setCurrentPayment(currentPayment.compareTo(payoffAmount) < 0 ? currentPayment : payoffAmount);
//        loanDto.setCurrentInterest(currentInterest.setScale(2, RoundingMode.HALF_EVEN));
//        loanDto.setPayoffAmount(payoffAmount);
//        loanDto.setCurrentBalance(currentBalance);
//        if (mostRecentStatement.getStatementDate() != null) {
//            loanDto.setLastStatementDate(mostRecentStatement.getStatementDate());
//            loanDto.setLastStatementBalance(mostRecentStatement.getEndingBalance());
//        }
//    }

}