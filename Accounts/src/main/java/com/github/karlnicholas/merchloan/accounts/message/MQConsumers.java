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
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import javax.annotation.PreDestroy;
import java.io.IOException;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final Connection consumerConnection;
    private final Connection producerConnection;
    private final AccountManagementService accountManagementService;
    private final RegisterManagementService registerManagementService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;
    private final MQConsumerUtils mqConsumerUtils;
    private final Channel loansToCyceReplyProducer;
    private final Channel accountBillingFeeChargeProducer;
    private final Channel accountBillingInterestChargeProducer;
    private final Channel queryAccountIdReplyProducer;
    private final Channel queryLoanIdReplyProducer;
    private final Channel createAccountProducer;
    private final Channel loanIdComputeProducer;
    private final Channel statementStatementHeaderProducer;
    private final Channel fundingMessageProducer;
    private final Channel validateCreditProducer;
    private final Channel validateDebitProducer;
    private final Channel closeLoanProducer;
    private final Channel loadClosedProducer;

    private static final String NULL_ERROR_MESSAGE = "Message body null";

    public MQConsumers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils, AccountManagementService accountManagementService, RegisterManagementService registerManagementService, QueryService queryService) throws Exception {
        consumerConnection = connectionFactory.newConnection();

        this.accountManagementService = accountManagementService;
        this.registerManagementService = registerManagementService;
        this.queryService = queryService;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mqConsumerUtils = mqConsumerUtils;

        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCreateAccountQueue(), false, this::createAccountMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountFundingQueue(), false, this::fundingMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateCreditQueue(), false, this::validateCreditMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateDebitQueue(), false, this::validateDebitMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCloseLoanQueue(), false, this::closeLoanMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountLoanClosedQueue(), false, this::loanClosedMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountStatementStatementHeaderQueue(), false, this::statementStatementHeaderMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountFeeChargeQueue(), false, this::accountBillingFeeChargeMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountInterestChargeQueue(), false, this::accountBillingInterestChargeMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryLoansToCycleQueue(), false, this::billingLoansToCyceMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryAccountIdQueue(), false, this::queryAccountIdMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryLoanIdQueue(), false, this::queryLoanIdMessage);
        mqConsumerUtils.bindConsumer(consumerConnection.createChannel(), mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountLoanIdComputeQueue(), false, this::accountLoanIdComputeMessage);

        producerConnection = connectionFactory.newConnection();
        fundingMessageProducer = producerConnection.createChannel();
        loansToCyceReplyProducer = producerConnection.createChannel();
        accountBillingFeeChargeProducer = producerConnection.createChannel();
        accountBillingInterestChargeProducer = producerConnection.createChannel();
        queryAccountIdReplyProducer = producerConnection.createChannel();
        queryLoanIdReplyProducer = producerConnection.createChannel();
        loanIdComputeProducer = producerConnection.createChannel();
        statementStatementHeaderProducer = producerConnection.createChannel();
        createAccountProducer = producerConnection.createChannel();
        validateCreditProducer = producerConnection.createChannel();
        validateDebitProducer = producerConnection.createChannel();
        closeLoanProducer = producerConnection.createChannel();
        loadClosedProducer = producerConnection.createChannel();

    }

    @PreDestroy
    public void preDestroy() throws IOException {
        log.info("consumers preDestroy");
        consumerConnection.close();
        producerConnection.close();
    }


    public void queryAccountIdMessage(String consumerTag, Delivery message) {
        UUID id = (UUID) SerializationUtils.deserialize(message.getBody());
        try {
            log.debug("receivedQueryAccountIdMessage {}", id);
            Optional<Account> accountOpt = queryService.queryAccountId(id);
            String response;
            if (accountOpt.isPresent()) {
                response = objectMapper.writeValueAsString(accountOpt.get());
            } else {
                response = "ERROR: id not found: " + id;
            }
            queryAccountIdReplyProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize(response));
        } catch (Exception ex) {
            log.error("receivedQueryAccountIdMessage exception", ex);
        }
    }

    public void queryLoanIdMessage(String consumerTag, Delivery message) {
        UUID id = (UUID) SerializationUtils.deserialize(message.getBody());
        try {
            log.debug("receivedQueryLoanIdMessage {}", id);
            Optional<LoanDto> r = queryService.queryLoanId(id);
            if (r.isPresent()) {
                // down the chain
                queryLoanIdReplyProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementQueryMostRecentStatementQueue(), message.getProperties(), SerializationUtils.serialize(r.get()));
            } else {
                queryLoanIdReplyProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize("ERROR: Loan not found for id: " + id));
            }
        } catch (Exception ex) {
            log.error("receivedQueryLoanIdMessage exception", ex);
            Thread.currentThread().interrupt();
        }
    }

    public void accountLoanIdComputeMessage(String consumerTag, Delivery message) {
        try {
            LoanDto loanDto = (LoanDto) SerializationUtils.deserialize(message.getBody());
            log.debug("receivedAccountLoanIdComputeMessage loanId: {}", loanDto.getLoanId());
            queryService.computeLoanValues(loanDto);
            loanIdComputeProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize(objectMapper.writeValueAsString(loanDto)));
        } catch (Exception ex) {
            log.error("receivedAccountLoanIdComputeMessage exception", ex);
        }
    }

    public void createAccountMessage(String consumerTag, Delivery message) {
        CreateAccount createAccount = (CreateAccount) SerializationUtils.deserialize(message.getBody());
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
                createAccountProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), message.getProperties(), SerializationUtils.serialize(requestResponse));
            } catch (IOException e) {
                log.error("receivedCreateAccountMessage exception", e);
            }
        }
    }

    public void fundingMessage(String consumerTag, Delivery message) {
        // M= P [r (1+r)^n/ ((1+r)^n)-1)]
        // r = .10 / 12 = 0.00833
        // 10000 * 0.00833(1.00833)^12 / ((1.00833)^12)-1]
        // 10000 * 0.0092059/0.104713067
        // 10000 * 0.08791548
        // = 879.16
        FundLoan fundLoan = (FundLoan) SerializationUtils.deserialize(message.getBody());
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
                fundingMessageProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), message.getProperties(), SerializationUtils.serialize(requestResponse));
            } catch (IOException e) {
                log.error("fundingMessage", e);
            }
        }
    }

    public void validateCreditMessage(String consumerTag, Delivery message) {
        CreditLoan creditLoan = (CreditLoan) SerializationUtils.deserialize(message.getBody());
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
                validateCreditProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), message.getProperties(), SerializationUtils.serialize(requestResponse));
            } catch (IOException e) {
                log.error("validateCreditMessage", e);
            }
        }
    }

    public void validateDebitMessage(String consumerTag, Delivery message) {
        DebitLoan debitLoan = (DebitLoan) SerializationUtils.deserialize(message.getBody());
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
                validateDebitProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), message.getProperties(), SerializationUtils.serialize(requestResponse));
            } catch (IOException e) {
                log.error("validateCreditMessage", e);
            }
        }
    }

    public void closeLoanMessage(String consumerTag, Delivery message) {
        CloseLoan closeLoan = (CloseLoan)  SerializationUtils.deserialize(message.getBody());
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
                    closeLoanProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementCloseStatementQueue(), message.getProperties(), SerializationUtils.serialize(statementHeader));
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
                closeLoanProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), message.getProperties(), SerializationUtils.serialize(requestResponse));
            } catch (IOException e) {
                log.error("validateCreditMessage", e);
            }
        }
    }

    public void billingLoansToCyceMessage(String consumerTag, Delivery message) {
        try {
            LocalDate businessDate = (LocalDate) SerializationUtils.deserialize(message.getBody());
            log.debug("receivedLoansToCyceMessage {}", businessDate);
            loansToCyceReplyProducer.basicPublish(mqConsumerUtils.getExchange(), message.getProperties().getReplyTo(), message.getProperties(), SerializationUtils.serialize(accountManagementService.loansToCycle(businessDate)));
        } catch (Exception ex) {
            log.error("receivedLoansToCyceMessage exception", ex);
        }
    }

    public void statementStatementHeaderMessage(String consumerTag, Delivery message) {
        try {
            StatementHeaderWork statementHeaderWork = (StatementHeaderWork) SerializationUtils.deserialize(message.getBody());
            log.debug("receivedStatementHeaderMessage loanId: {}", statementHeaderWork.getStatementHeader().getLoanId());
            ServiceRequestResponse requestResponse = accountManagementService.statementHeader(statementHeaderWork.getStatementHeader());
            log.debug("receivedStatementHeaderMessage serviceRequestResponse: {}", requestResponse);
            if (requestResponse.isSuccess()) {
                registerManagementService.setStatementHeaderRegisterEntryies(statementHeaderWork.getStatementHeader());
                statementStatementHeaderProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementContinueQueue(), message.getProperties(), SerializationUtils.serialize(statementHeaderWork));
            } else {
                statementStatementHeaderProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), message.getProperties(), SerializationUtils.serialize(requestResponse));
            }
        } catch (Exception ex) {
            log.error("receivedStatementHeaderMessage exception", ex);
        }
    }

    public void accountBillingFeeChargeMessage(String consumerTag, Delivery message) {
        try {
            StatementHeaderWork statementHeaderWork = persistBillingCycleCharge(message);
            log.debug("accountBillingFeeChargeMessage");
            accountBillingFeeChargeProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementContinue2Queue(), message.getProperties(), SerializationUtils.serialize(statementHeaderWork));
        } catch (Exception ex) {
            log.error("receivedBillingCycleChargeMessage exception", ex);
        }
    }

    public void accountBillingInterestChargeMessage(String consumerTag, Delivery message) {
        try {
            StatementHeaderWork statementHeaderWork = persistBillingCycleCharge(message);
            log.debug("accountBillingInterestChargeMessage");
            accountBillingInterestChargeProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementContinue2Queue(), message.getProperties(), SerializationUtils.serialize(statementHeaderWork));
        } catch (Exception ex) {
            log.error("receivedBillingCycleChargeMessage exception", ex);
        }
    }

    private StatementHeaderWork persistBillingCycleCharge(Delivery message) throws SQLException {
        StatementHeaderWork statementHeaderWork = (StatementHeaderWork)  SerializationUtils.deserialize(message.getBody());
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
        return statementHeaderWork;
    }

    public void loanClosedMessage(String consumerTag, Delivery message) {
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(message.getBody());
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
                loadClosedProducer.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getServicerequestQueue(), message.getProperties(), SerializationUtils.serialize(requestResponse));
            } catch (IOException e) {
                log.error("createAccountMessage", e);
            }
        }
    }
}
