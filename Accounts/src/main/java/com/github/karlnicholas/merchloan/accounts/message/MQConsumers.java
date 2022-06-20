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
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Delivery;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.SerializationUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class MQConsumers {
    private final AccountManagementService accountManagementService;
    private final RegisterManagementService registerManagementService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;
    private final MQProducers rabbitMqSender;
    private final MQConsumerUtils mqConsumerUtils;
    private final Channel responseChannel;
    private static final String NULL_ERROR_MESSAGE = "Message body null";

    public MQConsumers(Connection connection, MQProducers rabbitMqSender, MQConsumerUtils mqConsumerUtils, AccountManagementService accountManagementService, RegisterManagementService registerManagementService, QueryService queryService) throws IOException {
        this.accountManagementService = accountManagementService;
        this.registerManagementService = registerManagementService;
        this.queryService = queryService;
        this.rabbitMqSender = rabbitMqSender;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.mqConsumerUtils = mqConsumerUtils;

        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCreateAccountQueue(), false, this::receivedCreateAccountMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountFundingQueue(), false, this::receivedFundingMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateCreditQueue(), false, this::receivedValidateCreditMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateDebitQueue(), false, this::receivedValidateDebitMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCloseLoanQueue(), false, this::receivedCloseLoanMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountLoanClosedQueue(), false, this::receivedLoanClosedMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryStatementHeaderQueue(), false, this::receivedStatementHeaderMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountBillingCycleChargeQueue(), false, this::receivedBillingCycleChargeMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryLoansToCycleQueue(), false, this::receivedLoansToCyceMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryAccountIdQueue(), false, this::receivedQueryAccountIdMessage);
        mqConsumerUtils.bindConsumer(connection, mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountQueryLoanIdQueue(), false, this::receivedQueryLoanIdMessage);

        responseChannel = connection.createChannel();
    }

    public void receivedStatementHeaderMessage(String consumerTag, Delivery delivery) {
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(delivery.getBody());
        try {
            log.debug("receivedStatementHeaderMessage {}", statementHeader);
            ServiceRequestResponse serviceRequestResponse = accountManagementService.statementHeader(statementHeader);
            if (serviceRequestResponse.isSuccess())
                registerManagementService.setStatementHeaderRegisterEntryies(statementHeader);
            reply(delivery, statementHeader);
        } catch (Exception ex) {
            log.error("receivedStatementHeaderMessage exception {}", ex.getMessage());
        }
    }

    public void receivedLoansToCyceMessage(String consumerTag, Delivery delivery) {
        LocalDate businessDate = (LocalDate) SerializationUtils.deserialize(delivery.getBody());
        try {
            log.debug("receivedLoansToCyceMessage {}", businessDate);
            reply(delivery, accountManagementService.loansToCycle(businessDate));
        } catch (Exception ex) {
            log.error("receivedLoansToCyceMessage exception {}", ex.getMessage());
        }
    }

    public void receivedBillingCycleChargeMessage(String consumerTag, Delivery delivery) {
        BillingCycleCharge billingCycleCharge = (BillingCycleCharge) SerializationUtils.deserialize(delivery.getBody());
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
            reply(delivery, registerEntryMessage);
        } catch (Exception ex) {
            log.error("receivedBillingCycleChargeMessage exception {}", ex.getMessage());
        }
    }

    public void receivedQueryAccountIdMessage(String consumerTag, Delivery delivery) {
        UUID id = (UUID) SerializationUtils.deserialize(delivery.getBody());
        try {
            log.debug("receivedQueryAccountIdMessage {}", id);
            Optional<Account> accountOpt = queryService.queryAccountId(id);
            if (accountOpt.isPresent()) {
                reply(delivery, objectMapper.writeValueAsString(accountOpt.get()));
            } else {
                reply(delivery, "ERROR: id not found: " + id);
            }
        } catch (Exception ex) {
            log.error("receivedQueryAccountIdMessage exception {}", ex.getMessage());
        }
    }

    public void receivedQueryLoanIdMessage(String consumerTag, Delivery delivery) {
        UUID id = (UUID) SerializationUtils.deserialize(delivery.getBody());
        try {
            log.debug("receivedQueryLoanIdMessage {}", id);
            Optional<LoanDto> r = queryService.queryLoanId(id);
            if (r.isPresent()) {
                reply(delivery, objectMapper.writeValueAsString(r.get()));
            } else {
                reply(delivery, ("ERROR: Loan not found for id: " + id));
            }
        } catch (Exception ex) {
            log.error("receivedQueryLoanIdMessage exception {}", ex.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void reply(Delivery delivery, Object data) throws IOException {
        AMQP.BasicProperties replyProps = new AMQP.BasicProperties
                .Builder()
                .correlationId(delivery.getProperties().getCorrelationId())
                .build();
        responseChannel.basicPublish(mqConsumerUtils.getExchange(), delivery.getProperties().getReplyTo(), replyProps, SerializationUtils.serialize(data));
    }

    public void receivedCreateAccountMessage(String consumerTag, Delivery delivery) throws IOException {
        CreateAccount createAccount = (CreateAccount) SerializationUtils.deserialize(delivery.getBody());
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
            rabbitMqSender.serviceRequestServiceRequest(requestResponse);
        }
    }

    public void receivedFundingMessage(String consumerTag, Delivery delivery) throws IOException {
        // M= P [r (1+r)^n/ ((1+r)^n)-1)]
        // r = .10 / 12 = 0.00833
        // 10000 * 0.00833(1.00833)^12 / ((1.00833)^12)-1]
        // 10000 * 0.0092059/0.104713067
        // 10000 * 0.08791548
        // = 879.16
        FundLoan fundLoan = (FundLoan) SerializationUtils.deserialize(delivery.getBody());
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
            rabbitMqSender.serviceRequestServiceRequest(requestResponse);
        }
    }

    public void receivedValidateCreditMessage(String consumerTag, Delivery delivery) throws IOException {
        CreditLoan creditLoan = (CreditLoan) SerializationUtils.deserialize(delivery.getBody());
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
            rabbitMqSender.serviceRequestServiceRequest(requestResponse);
        }
    }

    public void receivedValidateDebitMessage(String consumerTag, Delivery delivery) throws IOException {
        DebitLoan debitLoan = (DebitLoan) SerializationUtils.deserialize(delivery.getBody());
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
            rabbitMqSender.serviceRequestServiceRequest(requestResponse);
        }
    }

    public void receivedCloseLoanMessage(String consumerTag, Delivery delivery) throws IOException {
        CloseLoan closeLoan = (CloseLoan) SerializationUtils.deserialize(delivery.getBody());
        if ( closeLoan == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse serviceRequestResponse = ServiceRequestResponse.builder().id(closeLoan.getId()).build();
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
                            , serviceRequestResponse);
                    // determine interest balance
                    registerManagementService.creditLoan(CreditLoan.builder()
                                    .id(closeLoan.getPaymentId())
                                    .loanId(closeLoan.getLoanId())
                                    .date(closeLoan.getDate())
                                    .amount(closeLoan.getAmount())
                                    .description("Payoff Payment")
                                    .build()
                            , serviceRequestResponse);
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
                    serviceRequestResponse.setFailure("PayoffAmount incorrect. Required: " + loanOpt.get().getPayoffAmount());
                }
            } else {
                serviceRequestResponse.setFailure("loan not found for id: " + closeLoan.getLoanId());
            }
        } catch (InterruptedException iex) {
            log.error("receivedCloseLoanMessage exception {}", iex.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception ex) {
            log.error("receivedCloseLoanMessage exception {}", ex.getMessage());
            serviceRequestResponse.setError("receivedCloseLoanMessage exception " + ex.getMessage());
        } finally {
            rabbitMqSender.serviceRequestServiceRequest(serviceRequestResponse);
        }
    }

    public void receivedLoanClosedMessage(String consumerTag, Delivery delivery) throws IOException {
        StatementHeader statementHeader = (StatementHeader) SerializationUtils.deserialize(delivery.getBody());
        if ( statementHeader == null ) {
            throw new IllegalStateException(NULL_ERROR_MESSAGE);
        }
        ServiceRequestResponse serviceRequestResponse = ServiceRequestResponse.builder().id(statementHeader.getId()).build();
        try {
            log.debug("receivedLoanClosedMessage {} ", statementHeader);
            accountManagementService.closeLoan(statementHeader.getLoanId());
            serviceRequestResponse.setSuccess();
        } catch (Exception ex) {
            log.error("receivedLoanClosedMessage exception {}", ex.getMessage());
            serviceRequestResponse.setError("receivedLoanClosedMessage excepion: " + ex.getMessage());
        } finally {
            rabbitMqSender.serviceRequestServiceRequest(serviceRequestResponse);
        }
    }

}
