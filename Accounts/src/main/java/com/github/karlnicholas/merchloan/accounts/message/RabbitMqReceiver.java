package com.github.karlnicholas.merchloan.accounts.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.karlnicholas.merchloan.accounts.model.Account;
import com.github.karlnicholas.merchloan.accounts.model.Loan;
import com.github.karlnicholas.merchloan.accounts.service.QueryService;
import com.github.karlnicholas.merchloan.jms.message.RabbitMqSender;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.accounts.service.AccountManagementService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class RabbitMqReceiver implements RabbitListenerConfigurer {
    private final AccountManagementService accountManagementService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;
    private final RabbitMqSender rabbitMqSender;

    public RabbitMqReceiver(AccountManagementService accountManagementService, QueryService queryService, RabbitMqSender rabbitMqSender) {
        this.accountManagementService = accountManagementService;
        this.queryService = queryService;
        this.rabbitMqSender = rabbitMqSender;
        this.objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Override
    public void configureRabbitListeners(RabbitListenerEndpointRegistrar rabbitListenerEndpointRegistrar) {
    }

    @RabbitListener(queues = "${rabbitmq.account.createaccount.queue}")
    public void receivedCreateAccountMessage(CreateAccount createAccount) {
        log.info("CreateAccount Details Received is.. {}", createAccount);
        ServiceRequestResponse serviceRequest = accountManagementService.createAccount(createAccount);
        rabbitMqSender.serviceRequestServiceRequest(serviceRequest);

    }

    @RabbitListener(queues = "${rabbitmq.account.funding.queue}")
    public void receivedFundingMessage(FundLoan fundLoan) {
        try {
            log.info("FundLoan Received {} ", fundLoan);
            ServiceRequestResponse serviceRequestResponse = accountManagementService.fundAccount(fundLoan);
            if ( serviceRequestResponse.getStatus() == ServiceRequestResponse.STATUS.SUCCESS ) {
                rabbitMqSender.registerFundLoan(
                        DebitLoan.builder()
                                .id(fundLoan.getId())
                                .amount(fundLoan.getAmount())
                                .date(fundLoan.getStartDate())
                                .loanId(fundLoan.getId())
                                .description(fundLoan.getDescription())
                                .build()
                );
            } else {
                rabbitMqSender.serviceRequestServiceRequest(serviceRequestResponse);
            }
        } catch ( Exception ex) {
            log.error("void receivedFundingMessage(FundLoan funding) {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.validate.credit.queue}")
    public void receivedValidateCreditMessage(CreditLoan creditLoan) {
        try {
            log.info("CreditLoan Received {} ", creditLoan);
            ServiceRequestResponse serviceRequest = accountManagementService.validateLoan(creditLoan.getLoanId());
            if ( serviceRequest.getStatus() == ServiceRequestResponse.STATUS.SUCCESS ) {
                rabbitMqSender.registerCreditLoan(
                        CreditLoan.builder()
                                .id(creditLoan.getId())
                                .amount(creditLoan.getAmount())
                                .date(creditLoan.getDate())
                                .loanId(creditLoan.getLoanId())
                                .description(creditLoan.getDescription())
                                .build()
                );
            } else {
                rabbitMqSender.serviceRequestServiceRequest(serviceRequest);
            }
        } catch ( Exception ex) {
            log.error("void receivedValidateCreditMessage(CreditLoan creditLoan) {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.validate.debit.queue}")
    public void receivedValidateDebitMessage(DebitLoan debitLoan) {
        try {
            log.info("DebitLoan Received {} ", debitLoan);
            ServiceRequestResponse serviceRequest = accountManagementService.validateLoan(debitLoan.getLoanId());
            if ( serviceRequest.getStatus() == ServiceRequestResponse.STATUS.SUCCESS ) {
                rabbitMqSender.registerDebitLoan(
                        DebitLoan.builder()
                                .id(debitLoan.getId())
                                .amount(debitLoan.getAmount())
                                .date(debitLoan.getDate())
                                .loanId(debitLoan.getLoanId())
                                .description(debitLoan.getDescription())
                                .build()
                );
            } else {
                rabbitMqSender.serviceRequestServiceRequest(serviceRequest);
            }
        } catch ( Exception ex) {
            log.error("void receivedValidateDebitMessage(DebitLoan debitLoan) {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.query.account.id.queue}")
    public String receivedQueryAccountIdMessage(UUID id) {
        try {
            log.info("QueryAccountId Received {}}", id);
            Optional<Account> r = queryService.queryAccountId(id);
            if ( r.isPresent() ) {
                return objectMapper.writeValueAsString(r.get());
            } else {
                return "ERROR: id not found: " + id;
            }
        } catch ( Exception ex) {
            log.error("String receivedQueryAccountIdMessage(UUID id) exception {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.query.loan.id.queue}")
    public String receivedQueryLoanIdMessage(UUID id) {
        try {
            log.info("QueryLoanId Received {}", id);
            Optional<Loan> r = queryService.queryLoanId(id);
            if ( r.isPresent() ) {
                return objectMapper.writeValueAsString(r.get());
            } else {
                return "ERROR: id not found: " + id;
            }
        } catch ( Exception ex) {
            log.error("String receivedQueryLoanIdMessage(UUID id) exception {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }
}