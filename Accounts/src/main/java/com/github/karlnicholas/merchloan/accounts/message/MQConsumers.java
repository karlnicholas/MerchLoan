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
    private final ClientProducer accountProducer;
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

        accountCreateAccountQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountCreateAccountQueue(), false, this::receivedCreateAccountMessage);
        accountFundingQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountFundingQueue(), false, this::receivedFundingMessage);
        accountValidateCreditQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountValidateCreditQueue(), false, this::receivedValidateCreditMessage);
        accountValidateDebitQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountValidateDebitQueue(), false, this::receivedValidateDebitMessage);
        accountCloseLoanQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountCloseLoanQueue(), false, this::receivedCloseLoanMessage);
        accountLoanClosedQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountLoanClosedQueue(), false, this::receivedLoanClosedMessage);
        accountQueryStatementHeaderQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryStatementHeaderQueue(), false, this::receivedStatementHeaderMessage);
        accountBillingCycleChargeQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountBillingCycleChargeQueue(), false, this::receivedBillingCycleChargeMessage);
        accountQueryLoansToCycleQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryLoansToCycleQueue(), false, this::receivedLoansToCyceMessage);
        accountQueryAccountIdQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryAccountIdQueue(), false, this::receivedQueryAccountIdMessage);
        accountQueryLoanIdQueue = mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryLoanIdQueue(), false, this::receivedQueryLoanIdMessage);

        accountProducer = clientSession.createProducer();

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
            log.debug("receivedStatementHeaderMessage {}", statementHeader);
            ServiceRequestResponse serviceRequestResponse = accountManagementService.statementHeader(statementHeader);
            if (serviceRequestResponse.isSuccess())
                registerManagementService.setStatementHeaderRegisterEntryies(statementHeader);
            reply(message, statementHeader);
        } catch (Exception ex) {
            log.error("receivedStatementHeaderMessage exception {}", ex.getMessage());
        }
    }

    public void receivedLoansToCyceMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        LocalDate businessDate = (LocalDate) SerializationUtils.deserialize(mo);
        try {
            log.debug("receivedLoansToCyceMessage {}", businessDate);
            reply(message, accountManagementService.loansToCycle(businessDate));
        } catch (Exception ex) {
            log.error("receivedLoansToCyceMessage exception {}", ex.getMessage());
        }
    }

    public void receivedBillingCycleChargeMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        BillingCycleCharge billingCycleCharge = (BillingCycleCharge)  SerializationUtils.deserialize(mo);
        try {
            log.debug("receivedBillingCycleChargeMessage {}", billingCycleCharge);
            RegisterEntry re = registerManagementService.billingCycleCharge(billingCycleCharge);
            RegisterEntryMessage registerEntryMessage = RegisterEntryMessage.builder()
                    .date(re.getDate())
                    .credit(re.getCredit())
                    .debit(re.getDebit())
                    .description(re.getDescription())
                    .timeStamp(re.getTimeStamp())
                    .build();
            reply(message, registerEntryMessage);
        } catch (Exception ex) {
            log.error("receivedBillingCycleChargeMessage exception {}", ex.getMessage());
        }
    }

    public void receivedQueryAccountIdMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        UUID id = (UUID) SerializationUtils.deserialize(mo);
        try {
            log.debug("receivedQueryAccountIdMessage {}", id);
            Optional<Account> accountOpt = queryService.queryAccountId(id);
            if (accountOpt.isPresent()) {
                reply(message, objectMapper.writeValueAsString(accountOpt.get()));
            } else {
                reply(message, "ERROR: id not found: " + id);
            }
        } catch (Exception ex) {
            log.error("receivedQueryAccountIdMessage exception {}", ex.getMessage());
        }
    }

    public void receivedQueryLoanIdMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        UUID id = (UUID) SerializationUtils.deserialize(mo);
        try {
            log.debug("receivedQueryLoanIdMessage {}", id);
            Optional<LoanDto> r = queryService.queryLoanId(id);
            if (r.isPresent()) {
                reply(message, objectMapper.writeValueAsString(r.get()));
            } else {
                reply(message, ("ERROR: Loan not found for id: " + id));
            }
        } catch (Exception ex) {
            log.error("receivedQueryLoanIdMessage exception {}", ex.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void reply(ClientMessage origMessage, Object data) throws ActiveMQException {
        ClientMessage message = clientSession.createMessage(false);
        message.setCorrelationID(origMessage.getCorrelationID());
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(data));
        accountProducer.send(origMessage.getReplyTo(), message, null);
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
            log.debug("receivedCreateAccountMessage{}", createAccount);
            accountManagementService.createAccount(createAccount, requestResponse);
        } catch (Exception ex) {
            log.error("receivedCreateAccountMessage exception {}", ex.getMessage());
            requestResponse.setError(ex.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
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
            log.debug("receivedFundingMessage {} ", fundLoan);
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
        } catch (Exception ex) {
            log.error("receivedFundingMessage exception {}", ex.getMessage());
            requestResponse.setError(ex.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
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
            log.debug("receivedValidateCreditMessage {} ", creditLoan);
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
        } catch (Exception ex) {
            log.error("receivedValidateCreditMessage exception {}", ex.getMessage());
            requestResponse.setError(ex.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
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
            log.debug("receivedValidateDebitMessage {} ", debitLoan);
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
        } catch (Exception ex) {
            log.error("receivedValidateDebitMessage exception {}", ex.getMessage());
            requestResponse.setError(ex.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
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
            log.debug("receivedCloseLoanMessage {} ", closeLoan);
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
        } catch (InterruptedException iex) {
            log.error("receivedCloseLoanMessage exception {}", iex.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            log.error("receivedCloseLoanMessage exception {}", ex.getMessage());
            requestResponse.setError("receivedCloseLoanMessage exception " + ex.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
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
            log.debug("receivedLoanClosedMessage {} ", statementHeader);
            accountManagementService.closeLoan(statementHeader.getLoanId());
            requestResponse.setSuccess();
        } catch (Exception ex) {
            log.error("receivedLoanClosedMessage exception {}", ex.getMessage());
            requestResponse.setError("receivedLoanClosedMessage excepion: " + ex.getMessage());
        } finally {
            try {
                mqProducers.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
