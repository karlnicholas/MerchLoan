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
import java.sql.SQLException;
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
    private final MQConsumerUtils mqConsumerUtils;
    private final ClientProducer loansToCyceReplyProducer;
    private final ClientProducer accountBillingFeeChargeProducer;
    private final ClientProducer accountBillingInterestChargeProducer;
    private final ClientProducer queryAccountIdReplyProducer;
    private final ClientProducer queryLoanIdReplyProducer;
    private final ClientProducer createAccountProducer;
    private final ClientProducer loanIdComputeProducer;
    private final ClientProducer statementStatementHeaderProducer;
    private final ClientProducer fundingMessageProducer;
    private final ClientProducer validateCreditProducer;
    private final ClientProducer validateDebitProducer;
    private final ClientProducer closeLoanProducer;
    private final ClientProducer loadClosedProducer;
    private final ClientConsumer accountCreateAccountQueue;
    private final ClientConsumer accountFundingQueue;
    private final ClientConsumer accountValidateCreditQueue;
    private final ClientConsumer accountValidateDebitQueue;
    private final ClientConsumer accountCloseLoanQueue;
    private final ClientConsumer accountLoanClosedQueue;
    private final ClientConsumer accountStatementStatementHeaderQueue;
    private final ClientConsumer accountFeeChargeQueue;
    private final ClientConsumer accountInterestChargeQueue;
    private final ClientConsumer accountQueryLoansToCycleQueue;
    private final ClientConsumer accountQueryAccountIdQueue;
    private final ClientConsumer accountQueryLoanIdQueue;
    private final ClientConsumer accountLoanIdComputeQueue;

    private static final String NULL_ERROR_MESSAGE = "Message body null";

    public MQConsumers(ServerLocator locator, MQConsumerUtils mqConsumerUtils, AccountManagementService accountManagementService, RegisterManagementService registerManagementService, QueryService queryService) throws Exception {
        ClientSessionFactory producerFactory =  locator.createSessionFactory();
        clientSession = producerFactory.createSession();
        clientSession.addMetaData(ClientSession.JMS_SESSION_IDENTIFIER_PROPERTY, "jms-client-id");
        clientSession.addMetaData("jms-client-id", "accounts-consumers");

        this.accountManagementService = accountManagementService;
        this.registerManagementService = registerManagementService;
        this.queryService = queryService;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mqConsumerUtils = mqConsumerUtils;

        accountCreateAccountQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountCreateAccountQueue()), false, this::createAccountMessage);
        accountFundingQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountFundingQueue()), false, this::fundingMessage);
        accountValidateCreditQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountValidateCreditQueue()), false, this::validateCreditMessage);
        accountValidateDebitQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountValidateDebitQueue()), false, this::validateDebitMessage);
        accountCloseLoanQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountCloseLoanQueue()), false, this::closeLoanMessage);
        accountLoanClosedQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountLoanClosedQueue()), false, this::loanClosedMessage);
        accountStatementStatementHeaderQueue = mqConsumerUtils.bindConsumer(clientSession,SimpleString.toSimpleString(mqConsumerUtils.getAccountStatementStatementHeaderQueue()), false, this::statementStatementHeaderMessage);
        accountFeeChargeQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountFeeChargeQueue()), false, this::accountBillingFeeChargeMessage);
        accountInterestChargeQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountInterestChargeQueue()), false, this::accountBillingInterestChargeMessage);
        accountQueryLoansToCycleQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryLoansToCycleQueue()), false, this::billingLoansToCyceMessage);
        accountQueryAccountIdQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryAccountIdQueue()), false, this::queryAccountIdMessage);
        accountQueryLoanIdQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountQueryLoanIdQueue()), false, this::queryLoanIdMessage);
        accountLoanIdComputeQueue = mqConsumerUtils.bindConsumer(clientSession, SimpleString.toSimpleString(mqConsumerUtils.getAccountLoanIdComputeQueue()), false, this::accountLoanIdComputeMessage);

        fundingMessageProducer = clientSession.createProducer();
        loansToCyceReplyProducer = clientSession.createProducer();
        accountBillingFeeChargeProducer = clientSession.createProducer();
        accountBillingInterestChargeProducer = clientSession.createProducer();
        queryAccountIdReplyProducer = clientSession.createProducer();
        queryLoanIdReplyProducer = clientSession.createProducer();
        loanIdComputeProducer = clientSession.createProducer();
        statementStatementHeaderProducer = clientSession.createProducer();
        createAccountProducer = clientSession.createProducer();
        validateCreditProducer = clientSession.createProducer();
        validateDebitProducer = clientSession.createProducer();
        closeLoanProducer = clientSession.createProducer();
        loadClosedProducer = clientSession.createProducer();

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
        accountStatementStatementHeaderQueue.close();
        accountFeeChargeQueue.close();
        accountInterestChargeQueue.close();
        accountQueryLoansToCycleQueue.close();
        accountQueryAccountIdQueue.close();
        accountQueryLoanIdQueue.close();
        accountLoanIdComputeQueue.close();
        clientSession.deleteQueue(mqConsumerUtils.getAccountCreateAccountQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountFundingQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountValidateCreditQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountValidateDebitQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountCloseLoanQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountLoanClosedQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountStatementStatementHeaderQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountFeeChargeQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountInterestChargeQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountQueryLoansToCycleQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountQueryAccountIdQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountQueryLoanIdQueue());
        clientSession.deleteQueue(mqConsumerUtils.getAccountLoanIdComputeQueue());
        clientSession.close();
    }


    public void queryAccountIdMessage(ClientMessage message) {
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

    public void queryLoanIdMessage(ClientMessage message) {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        UUID id = (UUID) SerializationUtils.deserialize(mo);
        try {
            log.debug("receivedQueryLoanIdMessage {}", id);
            Optional<LoanDto> r = queryService.queryLoanId(id);
            if (r.isPresent()) {
                // down the chain
                ClientMessage forwardMessage = clientSession.createMessage(false);
                forwardMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(r.get()));
                forwardMessage.setReplyTo(message.getReplyTo());
                forwardMessage.setCorrelationID(message.getCorrelationID());
                queryLoanIdReplyProducer.send(mqConsumerUtils.getStatementQueryMostRecentStatementQueue(), forwardMessage);
            } else {
                ClientMessage forwardMessage = clientSession.createMessage(false);
                forwardMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize("ERROR: Loan not found for id: " + id));
                forwardMessage.setCorrelationID(message.getCorrelationID());
                queryLoanIdReplyProducer.send(message.getReplyTo(), forwardMessage);
            }
        } catch (Exception ex) {
            log.error("receivedQueryLoanIdMessage exception", ex);
            Thread.currentThread().interrupt();
        }
    }

    public void accountLoanIdComputeMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            LoanDto loanDto = (LoanDto) SerializationUtils.deserialize(mo);
            log.debug("receivedAccountLoanIdComputeMessage loanId: {}", loanDto.getLoanId());
            queryService.computeLoanValues(loanDto);
            ClientMessage replyMessage = clientSession.createMessage(false);
            replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(objectMapper.writeValueAsString(loanDto)));
            replyMessage.setCorrelationID(message.getCorrelationID());
            loanIdComputeProducer.send(message.getReplyTo(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedAccountLoanIdComputeMessage exception", ex);
        }
    }

    public void createAccountMessage(ClientMessage message) {
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
                ClientMessage responseMessage = clientSession.createMessage(false);
                responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
                createAccountProducer.send(mqConsumerUtils.getServicerequestQueue(), responseMessage);
            } catch (ActiveMQException e) {
                log.error("createAccountMessage", e);
            }
        }
    }

    public void fundingMessage(ClientMessage message) {
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
                ClientMessage responseMessage = clientSession.createMessage(false);
                responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
                fundingMessageProducer.send(mqConsumerUtils.getServicerequestQueue(), responseMessage);
            } catch (ActiveMQException e) {
                log.error("fundingMessage", e);
            }
        }
    }

    public void validateCreditMessage(ClientMessage message) {
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
                ClientMessage responseMessage = clientSession.createMessage(false);
                responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
                validateCreditProducer.send(mqConsumerUtils.getServicerequestQueue(), responseMessage);
            } catch (ActiveMQException e) {
                log.error("validateCreditMessage", e);
            }
        }
    }

    public void validateDebitMessage(ClientMessage message) {
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
                ClientMessage responseMessage = clientSession.createMessage(false);
                responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
                validateDebitProducer.send(mqConsumerUtils.getServicerequestQueue(), responseMessage);
            } catch (ActiveMQException e) {
                log.error("validateCreditMessage", e);
            }
        }
    }

    public void closeLoanMessage(ClientMessage message) {
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
                    ClientMessage forwardMessage = clientSession.createMessage(false);
                    forwardMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
                    forwardMessage.setCorrelationID(message.getCorrelationID());
                    closeLoanProducer.send(mqConsumerUtils.getStatementCloseStatementQueue(), forwardMessage);
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
                ClientMessage responseMessage = clientSession.createMessage(false);
                responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
                closeLoanProducer.send(mqConsumerUtils.getServicerequestQueue(), responseMessage);
            } catch (ActiveMQException e) {
                log.error("validateCreditMessage", e);
            }
        }
    }

    public void billingLoansToCyceMessage(ClientMessage message) {
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

    public void statementStatementHeaderMessage(ClientMessage message) {
        try {
            byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
            message.getBodyBuffer().readBytes(mo);
            StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(mo);
            log.debug("receivedStatementHeaderMessage loanId: {}", statementHeaderWork.getStatementHeader().getLoanId());
            ServiceRequestResponse requestResponse = accountManagementService.statementHeader(statementHeaderWork.getStatementHeader());
            log.debug("receivedStatementHeaderMessage serviceRequestResponse: {}", requestResponse);
            if (requestResponse.isSuccess()) {
                registerManagementService.setStatementHeaderRegisterEntryies(statementHeaderWork.getStatementHeader());
                ClientMessage continueMessage = clientSession.createMessage(false);
                continueMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeaderWork));
                continueMessage.setReplyTo(message.getReplyTo());
                continueMessage.setCorrelationID(message.getCorrelationID());
                statementStatementHeaderProducer.send(mqConsumerUtils.getStatementContinueQueue(), continueMessage);
            } else {
                ClientMessage replyMessage = clientSession.createMessage(false);
                replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
                replyMessage.setCorrelationID(message.getCorrelationID());
                statementStatementHeaderProducer.send(message.getReplyTo(), replyMessage);
            }
        } catch (Exception ex) {
            log.error("receivedStatementHeaderMessage exception", ex);
        }
    }

    public void accountBillingFeeChargeMessage(ClientMessage message) {
        try {
            ClientMessage replyMessage = persistBillingCycleCharge(message);
            log.debug("accountBillingFeeChargeMessage");
            accountBillingFeeChargeProducer.send(mqConsumerUtils.getStatementContinue2Queue(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedBillingCycleChargeMessage exception", ex);
        }
    }

    public void accountBillingInterestChargeMessage(ClientMessage message) {
        try {
            ClientMessage replyMessage = persistBillingCycleCharge(message);
            log.debug("accountBillingInterestChargeMessage");
            accountBillingInterestChargeProducer.send(mqConsumerUtils.getStatementContinue3Queue(), replyMessage);
        } catch (Exception ex) {
            log.error("receivedBillingCycleChargeMessage exception", ex);
        }
    }

    private ClientMessage persistBillingCycleCharge(ClientMessage message) throws SQLException {
        byte[] mo = new byte[message.getBodyBuffer().readableBytes()];
        message.getBodyBuffer().readBytes(mo);
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork)  SerializationUtils.deserialize(mo);
        log.debug("receivedBillingCycleChargeMessage: loanId: {}", statementHeaderWork.getBillingCycleCharge().getLoanId());
        RegisterEntry re = registerManagementService.billingCycleCharge(statementHeaderWork.getBillingCycleCharge());
        RegisterEntryMessage registerEntryMessage = RegisterEntryMessage.builder()
                .date(re.getDate())
                .credit(re.getCredit())
                .debit(re.getDebit())
                .description(re.getDescription())
                .timeStamp(re.getTimeStamp())
                .build();
        statementHeaderWork.getStatementHeader().getRegisterEntries().add(registerEntryMessage);
        ClientMessage replyMessage = clientSession.createMessage(false);
        replyMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeaderWork));
        replyMessage.setCorrelationID(message.getCorrelationID());
        return replyMessage;
    }

    public void loanClosedMessage(ClientMessage message) {
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
                ClientMessage responseMessage = clientSession.createMessage(false);
                responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(requestResponse));
                loadClosedProducer.send(mqConsumerUtils.getServicerequestQueue(), responseMessage);
            } catch (ActiveMQException e) {
                log.error("createAccountMessage", e);
            }
        }
    }
//    public Object mostRecentStatement(UUID loanId) throws ActiveMQException, InterruptedException {
//        log.debug("queryMostRecentStatement: {}", loanId);
//        String responseKey = UUID.randomUUID().toString();
//        replyWaitingHandlerMostRecentStatement.put(responseKey, loanId);
//        ClientMessage message = clientSession.createMessage(false);
//        message.setReplyTo(mostRecentStatementReplyQueueName);
//        message.setCorrelationID(responseKey);
//        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(loanId));
//        statementQueryMostRecentStatementProducer.send(message);
//        return replyWaitingHandlerMostRecentStatement.getReply(responseKey);
////        ClientMessage reply = mostRecentStatementReplyConsumer.receive();
////        byte[] mo = new byte[reply.getBodyBuffer().readableBytes()];
////        reply.getBodyBuffer().readBytes(mo);
////        return SerializationUtils.deserialize(mo);
//    }
//    public void closeStatement(StatementHeader statementHeader) throws ActiveMQException {
//        log.debug("statementCloseStatement: loanId: {}", statementHeader.getLoanId());
//        ClientMessage message = clientSession.createMessage(false);
//        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
//        closeStatementProducer.send(message);
//    }
//
//    public void serviceRequestServiceRequest(ClientMessage message, ServiceRequestResponse serviceRequest, ClientProducer producer) {
//        try {
//            log.debug("serviceRequestServiceRequest: {}", serviceRequest.getId());
//            ClientMessage responseMessage = clientSession.createMessage(false);
//            responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
//            responseMessage.setCorrelationID(message.getCorrelationID());
//            producer.send(message.getReplyTo(), responseMessage);
//        } catch (ActiveMQException e) {
//            log.error("receivedFundingMessage exception", e);
//        }
//    }
//
//    public void serviceRequestServiceRequest(ClientMessage message, ServiceRequestResponse serviceRequest, ClientProducer producer) {
//        try {
//            log.debug("serviceRequestServiceRequest: {}", serviceRequest.getId());
//            ClientMessage responseMessage = clientSession.createMessage(false);
//            responseMessage.getBodyBuffer().writeBytes(SerializationUtils.serialize(serviceRequest));
//            responseMessage.setCorrelationID(message.getCorrelationID());
//            producer.send(message.getReplyTo(), responseMessage);
//        } catch (ActiveMQException e) {
//            log.error("receivedFundingMessage exception", e);
//        }
//    }
}
