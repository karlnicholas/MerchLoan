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
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

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
    private final MQProducers rabbitMqSender;
    private final MQConsumerUtils mqConsumerUtils;
    private final ClientProducer accountProducer;
    private static final String NULL_ERROR_MESSAGE = "Message body null";

    public MQConsumers(ClientSession clientSession, MQProducers rabbitMqSender, MQConsumerUtils mqConsumerUtils, AccountManagementService accountManagementService, RegisterManagementService registerManagementService, QueryService queryService) throws ActiveMQException {
        this.clientSession = clientSession;
        this.accountManagementService = accountManagementService;
        this.registerManagementService = registerManagementService;
        this.queryService = queryService;
        this.rabbitMqSender = rabbitMqSender;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mqConsumerUtils = mqConsumerUtils;

        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountCreateAccountQueue(), this::receivedCreateAccountMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountFundingQueue(), this::receivedFundingMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountValidateCreditQueue(), this::receivedValidateCreditMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountValidateDebitQueue(), this::receivedValidateDebitMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountCloseLoanQueue(), this::receivedCloseLoanMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountLoanClosedQueue(), this::receivedLoanClosedMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryStatementHeaderQueue(), this::receivedStatementHeaderMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountBillingCycleChargeQueue(), this::receivedBillingCycleChargeMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryLoansToCycleQueue(), this::receivedLoansToCyceMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryAccountIdQueue(), this::receivedQueryAccountIdMessage);
        mqConsumerUtils.bindConsumer(clientSession, mqConsumerUtils.getAccountQueryLoanIdQueue(), this::receivedQueryLoanIdMessage);

        accountProducer = clientSession.createProducer();
    }

    public void receivedStatementHeaderMessage(ClientMessage message) {
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
        try {
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
        LocalDate businessDate = (LocalDate) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
        try {
            log.debug("receivedLoansToCyceMessage {}", businessDate);
            reply(message, accountManagementService.loansToCycle(businessDate));
        } catch (Exception ex) {
            log.error("receivedLoansToCyceMessage exception {}", ex.getMessage());
        }
    }

    public void receivedBillingCycleChargeMessage(ClientMessage message) {
        BillingCycleCharge billingCycleCharge = (BillingCycleCharge) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
        UUID id = (UUID) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
        UUID id = (UUID) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
        accountProducer.send(origMessage.getReplyTo(), message);
    }

    public void receivedCreateAccountMessage(ClientMessage message) {
        CreateAccount createAccount = (CreateAccount) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
                rabbitMqSender.serviceRequestServiceRequest(requestResponse);
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
        FundLoan fundLoan = (FundLoan) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
                rabbitMqSender.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void receivedValidateCreditMessage(ClientMessage message) {
        CreditLoan creditLoan = (CreditLoan) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
                rabbitMqSender.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void receivedValidateDebitMessage(ClientMessage message) {
        DebitLoan debitLoan = (DebitLoan) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
                rabbitMqSender.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void receivedCloseLoanMessage(ClientMessage message) {
        CloseLoan closeLoan = (CloseLoan) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
                    rabbitMqSender.statementCloseStatement(statementHeader);
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
                rabbitMqSender.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void receivedLoanClosedMessage(ClientMessage message) {
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(message.getBodyBuffer().toByteBuffer().array());
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
                rabbitMqSender.serviceRequestServiceRequest(requestResponse);
            } catch (ActiveMQException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
