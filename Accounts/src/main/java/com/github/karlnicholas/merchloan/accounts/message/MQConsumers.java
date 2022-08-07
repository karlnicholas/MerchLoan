package com.github.karlnicholas.merchloan.accounts.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.karlnicholas.merchloan.accounts.model.Account;
import com.github.karlnicholas.merchloan.accounts.model.RegisterEntry;
import com.github.karlnicholas.merchloan.accounts.service.AccountManagementService;
import com.github.karlnicholas.merchloan.accounts.service.QueryService;
import com.github.karlnicholas.merchloan.accounts.service.RegisterManagementService;
import com.github.karlnicholas.merchloan.dto.LoanDto;
import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.*;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final ClientSession clientSession;
    private final AccountManagementService accountManagementService;
    private final RegisterManagementService registerManagementService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;
    private final MQProducers mqProducers;
    private final MQConsumerUtils mqConsumerUtils;
    private final ClientProducer statementHeaderReplyProducer;
    private final ClientProducer loansToCyceReplyProducer;
    private final ClientProducer billingCycleChargeReplyProducer;
    private final ClientProducer queryAccountIdReplyProducer;
    private final ClientProducer queryLoanIdReplyProducer;
    private final ClientConsumer accountCreateAccountQueue;
    private final ClientConsumer accountFundingQueue;
    private final ClientConsumer accountValidateCreditQueue;
    private final ClientConsumer accountValidateDebitQueue;
    private final ClientConsumer accountCloseLoanQueue;
    private final ClientConsumer accountLoanClosedQueue;
    private final ClientConsumer accountQueryStatementHeaderQueue;
    private final ClientConsumer accountBillingCycleChargeQueue;
    private final ClientConsumer accountQueryLoansToCycleQueue;
    private final ClientConsumer accountQueryAccountIdQueue;
    private final ClientConsumer accountQueryLoanIdQueue;

    private static final String NULL_ERROR_MESSAGE = "Message body null";

    public MQConsumers(ServerLocator locator, MQProducers mqProducers, MQConsumerUtils mqConsumerUtils, AccountManagementService accountManagementService, RegisterManagementService registerManagementService, QueryService queryService) throws Exception {
        ClientSessionFactory producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "accounts-consumers");

        this.accountManagementService = accountManagementService;
        this.registerManagementService = registerManagementService;
        this.queryService = queryService;
        this.mqProducers = mqProducers;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mqConsumerUtils = mqConsumerUtils;

        accountCreateAccountQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountCreateAccountQueue()), false, this::receivedCreateAccountMessage);
        accountFundingQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountFundingQueue()), false, this::receivedFundingMessage);
        accountValidateCreditQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountValidateCreditQueue()), false, this::receivedValidateCreditMessage);
        accountValidateDebitQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountValidateDebitQueue()), false, this::receivedValidateDebitMessage);
        accountCloseLoanQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountCloseLoanQueue()), false, this::receivedCloseLoanMessage);
        accountLoanClosedQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountLoanClosedQueue()), false, this::receivedLoanClosedMessage);
        accountQueryStatementHeaderQueue = mqConsumerUtils.bindConsumer(clientSession,SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryStatementHeaderQueue()), false, this::receivedStatementHeaderMessage);
        accountBillingCycleChargeQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountBillingCycleChargeQueue()), false, this::receivedBillingCycleChargeMessage);
        accountQueryLoansToCycleQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryLoansToCycleQueue()), false, this::receivedLoansToCyceMessage);
        accountQueryAccountIdQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryAccountIdQueue()), false, this::receivedQueryAccountIdMessage);
        accountQueryLoanIdQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryLoanIdQueue()), false, this::receivedQueryLoanIdMessage);

        statementHeaderReplyProducer = clientSession.createProducer();
        loansToCyceReplyProducer = clientSession.createProducer();
        billingCycleChargeReplyProducer = clientSession.createProducer();
        queryAccountIdReplyProducer = clientSession.createProducer();
        queryLoanIdReplyProducer = clientSession.createProducer();

        clientSession.start();
    }

    @PreDestroy
    public void preDestroy() throws ActiveMQException {
        log.info("consumers preDestroy");
        accountCreateAccountQueue.close();
        accountFundingQueue.close();
        accountValidateCreditQueue.close();
        accountValidateDebitQueue.close();
        accountCloseLoanQueue.close();
        accountLoanClosedQueue.close();
        accountQueryStatementHeaderQueue.close();
        accountBillingCycleChargeQueue.close();
        accountQueryLoansToCycleQueue.close();
        accountQueryAccountIdQueue.close();
        accountQueryLoanIdQueue.close();
        clientSession.deleteQueue(mqConsumerUtils.getAccountCreateAccountQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountFundingQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountValidateCreditQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountValidateDebitQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountCloseLoanQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountLoanClosedQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountQueryStatementHeaderQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountBillingCycleChargeQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountQueryLoansToCycleQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountQueryAccountIdQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountQueryLoanIdQueue());
        clientSession.close();
    }

    public void receivedStatementHeaderMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(mo);
            log.debug("receivedStatementHeaderMessage loanId: {}", statementHeader.getLoanId());
            ServiceRequestResponse serviceRequestResponse = accountManagementService.statementHeader(statementHeader);
            log.debug("receivedStatementHeaderMessage serviceRequestResponse: {}", serviceRequestResponse);
            if (serviceRequestResponse.isSuccess())
                registerManagementService.setStatementHeaderRegisterEntryies(statementHeader);
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
            replyMessage.setCorrelationID(message.getCorrelationID());
            statementHeaderReplyProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedStatementHeaderMessage exception", ex);
        }
    }

    public void receivedLoansToCyceMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            LocalDate businessDate = (LocalDate) SerializationUtils.deserialize(mo);
            log.debug("receivedLoansToCyceMessage {}", businessDate);
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(accountManagementService.loansToCycle(businessDate)));
            replyMessage.setCorrelationID(message.getCorrelationID());
            loansToCyceReplyProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedLoansToCyceMessage exception", ex);
        }
    }

    public void receivedBillingCycleChargeMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            BillingCycleCharge billingCycleCharge = (BillingCycleCharge)  SerializationUtils.deserialize(mo);
            log.debug("receivedBillingCycleChargeMessage: loanId: {}", billingCycleCharge.getLoanId());
            RegisterEntry re = registerManagementService.billingCycleCharge(billingCycleCharge);
            RegisterEntryMessage registerEntryMessage = RegisterEntryMessage.builder()
                    .date(re.getDate())
                    .credit(re.getCredit())
                    .debit(re.getDebit())
                    .description(re.getDescription())
                    .timeStamp(re.getTimeStamp())
                    .build();
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(registerEntryMessage));
            replyMessage.setCorrelationID(message.getCorrelationID());
            billingCycleChargeReplyProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedBillingCycleChargeMessage exception", ex);
        }
    }

    public void receivedQueryAccountIdMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        UUID id = (UUID) SerializationUtils.deserialize(mo);
        try {
            log.debug("receivedQueryAccountIdMessage {}", id);
            Optional<Account> accountOpt = queryService.queryAccountId(id);
            String response;
            if (accountOpt.isPresent()) {
                response = objectMapper.writeValueAsString(accountOpt.get());
            } else {
                response = "ERROR: id not found: " + id;
            }
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(response));
            replyMessage.setCorrelationID(message.getCorrelationID());
            queryAccountIdReplyProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedQueryAccountIdMessage exception", ex);
        }
    }

    public void receivedQueryLoanIdMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        UUID id = (UUID) SerializationUtils.deserialize(mo);
        try {
            log.debug("receivedQueryLoanIdMessage {}", id);
            Optional<LoanDto> r = queryService.queryLoanId(id);
            String response;
            if (r.isPresent()) {
                response = objectMapper.writeValueAsString(r.get());
            } else {
                response = "ERROR: Loan not found for id: " + id;
            }
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(response));
            replyMessage.setCorrelationID(message.getCorrelationID());
            queryLoanIdReplyProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedQueryLoanIdMessage exception", ex);
            Thread.currentThread().interrupt();
        }
    }

    public void receivedCreateAccountMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        CreateAccount createAccount = (CreateAccount) SerializationUtils.deserialize(mo);
        if ( createAccount == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder().id(createAccount.getId()).build();
        try {
            log.debug("receivedCreateAccountMessage{}", createAccount.getId());
            accountManagementService.createAccount(createAccount, requestResponse);
        } catch (Exception e) {
            log.error("receivedCreateAccountMessage exception", e);
            requestResponse.setError(e.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                log.error("receivedCreateAccountMessage exception", e);
            }
        }
    }

    public void receivedFundingMessage(ClientMessage message) {
        // M= P [r (1+r)^n/ ((1+r)^n)-1)]
        // r = .10 / 12 = 0.00833
        // 10000 * 0.00833(1.00833)^12 / ((1.00833)^12)-1]
        // 10000 * 0.0092059/0.104713067
        // 10000 * 0.08791548
        // = 879.16
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        FundLoan fundLoan = (FundLoan) SerializationUtils.deserialize(mo);
        if ( fundLoan == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(fundLoan.getId())
                .build();
        try {
            log.debug("receivedFundingMessage: accountId: {} ", fundLoan.getAccountId());
            accountManagementService.fundAccount(fundLoan, requestResponse);
            if (requestResponse.isSuccess()) {
                registerManagementService.fundLoan(
                        DebitLoan.builder()
                                .id(fundLoan.getId())
                                .amount(fundLoan.getAmount())
                                .date(fundLoan.getStartDate())
                                .loanId(fundLoan.getId())
                                .description(fundLoan.getDescription())
                                .build(),
                        requestResponse);
            }
        } catch (Exception e) {
            log.error("receivedFundingMessage exception", e);
            requestResponse.setError(e.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                log.error("receivedFundingMessage exception", e);
            }
        }
    }

    public void receivedValidateCreditMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        CreditLoan creditLoan = (CreditLoan) SerializationUtils.deserialize(mo);
        if ( creditLoan == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(creditLoan.getId())
                .build();
        try {
            log.debug("receivedValidateCreditMessage: loanId {} ", creditLoan.getLoanId());
            accountManagementService.validateLoan(creditLoan.getLoanId(), requestResponse);
            if (requestResponse.isSuccess()) {
                registerManagementService.creditLoan(CreditLoan.builder()
                        .id(creditLoan.getId())
                        .amount(creditLoan.getAmount())
                        .date(creditLoan.getDate())
                        .loanId(creditLoan.getLoanId())
                        .description(creditLoan.getDescription())
                        .build(), requestResponse);
            }
        } catch (Exception e) {
            log.error("receivedValidateCreditMessage exception", e);
            requestResponse.setError(e.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                log.error("receivedValidateCreditMessage exception", e);
            }
        }
    }

    public void receivedValidateDebitMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        DebitLoan debitLoan = (DebitLoan) SerializationUtils.deserialize(mo);
        if ( debitLoan == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(debitLoan.getId())
                .build();
        try {
            log.debug("receivedValidateDebitMessage: loanId: {} ", debitLoan.getLoanId());
            accountManagementService.validateLoan(debitLoan.getLoanId(), requestResponse);
            if (requestResponse.isSuccess()) {
                registerManagementService.debitLoan(DebitLoan.builder()
                                .id(debitLoan.getId())
                                .amount(debitLoan.getAmount())
                                .date(debitLoan.getDate())
                                .loanId(debitLoan.getLoanId())
                                .description(debitLoan.getDescription())
                                .build(),
                        requestResponse);
            }
        } catch (Exception e) {
            log.error("receivedValidateDebitMessage exception", e);
            requestResponse.setError(e.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                log.error("receivedValidateDebitMessage exception", e);
            }
        }
    }

    public void receivedCloseLoanMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        CloseLoan closeLoan = (CloseLoan)  SerializationUtils.deserialize(mo);
        if ( closeLoan == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder().id(closeLoan.getId()).build();
        try {
            log.debug("receivedCloseLoanMessage: loanId: {} ", closeLoan.getLoanId());
            Optional<LoanDto> loanOpt = queryService.queryLoanId(closeLoan.getLoanId());
            if (loanOpt.isPresent()) {
                if (closeLoan.getAmount().compareTo(loanOpt.get().getPayoffAmount()) == 0) {
                    registerManagementService.debitLoan(DebitLoan.builder()
                                    .id(closeLoan.getInterestChargeId())
                                    .loanId(closeLoan.getLoanId())
                                    .date(closeLoan.getDate())
                                    .amount(loanOpt.get().getCurrentInterest())
                                    .description("Interest")
                                    .build()
                            , requestResponse);
                    // determine interest balance
                    registerManagementService.creditLoan(CreditLoan.builder()
                                    .id(closeLoan.getPaymentId())
                                    .loanId(closeLoan.getLoanId())
                                    .date(closeLoan.getDate())
                                    .amount(closeLoan.getAmount())
                                    .description("Payoff Payment")
                                    .build()
                            , requestResponse);
                    closeLoan.setLoanDto(loanOpt.get());
                    closeLoan.setLastStatementDate(loanOpt.get().getLastStatementDate());
                    StatementHeader statementHeader = StatementHeader.builder()
                            .id(closeLoan.getId())
                            .accountId(closeLoan.getLoanDto().getAccountId())
                            .loanId(closeLoan.getLoanId())
                            .statementDate(closeLoan.getDate())
                            .startDate(closeLoan.getLastStatementDate().plusDays(1))
                            .endDate(closeLoan.getDate())
                            .build();
                    registerManagementService.setStatementHeaderRegisterEntryies(statementHeader);
                    mqProducers.statementCloseStatement(statementHeader);
                } else {
                    requestResponse.setFailure("PayoffAmount incorrect. Required: " + loanOpt.get().getPayoffAmount());
                }
            } else {
                requestResponse.setFailure("loan not found for id: " + closeLoan.getLoanId());
            }
        } catch (InterruptedException e) {
            log.error("receivedCloseLoanMessage exception", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            log.error("receivedCloseLoanMessage exception", e);
            requestResponse.setError("receivedCloseLoanMessage exception " + e.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                log.error("receivedCloseLoanMessage exception", e);
            }
        }
    }

    public void receivedLoanClosedMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(mo);
        if ( statementHeader == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder().id(statementHeader.getId()).build();
        try {
            log.debug("receivedLoanClosedMessage: loanId: {} ", statementHeader.getLoanId());
            accountManagementService.closeLoan(statementHeader.getLoanId());
            requestResponse.setSuccess();
        } catch (Exception e) {
            log.error("receivedLoanClosedMessage exception", e);
            requestResponse.setError("receivedLoanClosedMessage exception: " + e.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                log.error("receivedLoanClosedMessage exception", e);
            }
        }
    }
}
