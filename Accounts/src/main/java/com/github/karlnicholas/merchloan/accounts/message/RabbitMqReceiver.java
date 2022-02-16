package com.github.karlnicholas.merchloan.accounts.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.karlnicholas.merchloan.accounts.model.Account;
import com.github.karlnicholas.merchloan.accounts.service.QueryService;
import com.github.karlnicholas.merchloan.apimessage.message.BillingCycleChargeRequest;
import com.github.karlnicholas.merchloan.apimessage.message.CreditRequest;
import com.github.karlnicholas.merchloan.apimessage.message.DebitRequest;
import com.github.karlnicholas.merchloan.dto.LoanDto;
import com.github.karlnicholas.merchloan.jms.message.RabbitMqSender;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.accounts.service.AccountManagementService;
import com.github.karlnicholas.merchloan.redis.component.RedisComponent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class RabbitMqReceiver implements RabbitListenerConfigurer {
    private final AccountManagementService accountManagementService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;
    private final RedisComponent redisComponent;
    private final RabbitMqSender rabbitMqSender;


    public RabbitMqReceiver(AccountManagementService accountManagementService, QueryService queryService, RedisComponent redisComponent, RabbitMqSender rabbitMqSender) {
        this.accountManagementService = accountManagementService;
        this.queryService = queryService;
        this.redisComponent = redisComponent;
        this.rabbitMqSender = rabbitMqSender;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Override
    public void configureRabbitListeners(RabbitListenerEndpointRegistrar rabbitListenerEndpointRegistrar) {
    }

    @RabbitListener(queues = "${rabbitmq.account.createaccount.queue}")
    public void receivedCreateAccountMessage(CreateAccount createAccount) {
        try {
            log.info("CreateAccount Details Received is.. {}", createAccount);
            ServiceRequestResponse serviceRequest = accountManagementService.createAccount(createAccount);
            rabbitMqSender.serviceRequestServiceRequest(serviceRequest);
        } catch ( Exception ex) {
            log.error("void receivedCreateAccountMessage(CreateAccount createAccount) {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.funding.queue}")
    public void receivedFundingMessage(FundLoan fundLoan) {
        // M= P [r (1+r)^n/ ((1+r)^n)-1)]
        // r = .10 / 12 = 0.00833
        // 10000 * 0.00833(1.00833)^12 / ((1.00833)^12)-1]
        // 10000 * 0.0092059/0.104713067
        // 10000 * 0.08791548
        // = 879.16
        try {
            log.info("FundLoan Received {} ", fundLoan);
            ServiceRequestResponse serviceRequestResponse = accountManagementService.fundAccount(fundLoan);
            if ( serviceRequestResponse.isSuccess() ) {
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
            ServiceRequestResponse serviceRequestResponse = accountManagementService.validateLoan(creditLoan.getLoanId());
            if ( serviceRequestResponse.isSuccess() ) {
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
                rabbitMqSender.serviceRequestServiceRequest(serviceRequestResponse);
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
            ServiceRequestResponse serviceRequestResponse = accountManagementService.validateLoan(debitLoan.getLoanId());
            if ( serviceRequestResponse.isSuccess() ) {
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
                rabbitMqSender.serviceRequestServiceRequest(serviceRequestResponse);
            }
        } catch ( Exception ex) {
            log.error("void receivedValidateDebitMessage(DebitLoan debitLoan) {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.statementheader.queue}")
    public StatementHeader receivedStatementHeaderMessage(StatementHeader statementHeader) {
        try {
            log.info("StatementHeader Received {}", statementHeader);
            ServiceRequestResponse serviceRequestResponse = accountManagementService.statementHeader(statementHeader);
            if ( serviceRequestResponse.isSuccess() )
                statementHeader = (StatementHeader) rabbitMqSender.registerStatementHeader(statementHeader);
            return statementHeader;
        } catch ( Exception ex) {
            log.error("String receivedQueryLoanIdMessage(UUID id) exception {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.loanstocycle.queue}")
    public List<BillingCycle> receivedLoansToCyceMessage(LocalDate businessDate) {
        try {
            log.info("LoansToCyce Received {}", businessDate);
            return accountManagementService.loansToCycle(businessDate);
        } catch ( Exception ex) {
            log.error("String receivedQueryLoanIdMessage(UUID id) exception {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.account.closeloan.queue}")
    public void receivedCloseLoanMessage(CloseLoan closeLoan) {
        ServiceRequestResponse serviceRequestResponse = ServiceRequestResponse.builder().id(closeLoan.getId()).build();
        try {
            log.info("DebitLoan Received {} ", closeLoan);
            Optional<LoanDto> loanOpt = queryService.queryLoanId(closeLoan.getLoanId());
            if (loanOpt.isPresent()) {
                if ( closeLoan.getAmount().compareTo(loanOpt.get().getPayoffAmount()) == 0 ) {
//                    accountManagementService.closeLoan(closeLoan.getLoanId());
                    rabbitMqSender.serviceRequestBillingCycleCharge(BillingCycleChargeRequest.builder()
                            .date(closeLoan.getDate())
                            .debitRequest(new DebitRequest(closeLoan.getLoanId(), loanOpt.get().getCurrentInterest(), "Interest"))
                            .build()
                    );
                    // determine interest balance
                    rabbitMqSender.serviceRequestBillingCycleCharge(BillingCycleChargeRequest.builder()
                            .date(closeLoan.getDate())
                                    .creditRequest(new CreditRequest(closeLoan.getLoanId(), closeLoan.getAmount(), "Payoff Payment"))
                            .build()
                    );
                    // wait for responses
                    int responseCount = 2;
                    int sixtySeconds = 60;
                    while ( sixtySeconds > 0 ) {
                        Thread.sleep(1000);
                        if ( redisComponent.countChargeCompleted(closeLoan.getLoanId()) == responseCount ) {
                            break;
                        }
                        sixtySeconds--;
                    }
// UNDO
                    closeLoan.setLoanDto(loanOpt.get());
                    closeLoan.setLastStatementDate(loanOpt.get().getLastStatementDate());
                    rabbitMqSender.registerCloseLoan(closeLoan);
                } else {
                    serviceRequestResponse.setFailure("PayoffAmount incorrect. Required: " + loanOpt.get().getPayoffAmount());
                    rabbitMqSender.serviceRequestServiceRequest(serviceRequestResponse);
                }
            } else {
                serviceRequestResponse.setFailure("loan not found for id: " + closeLoan.getLoanId());
                rabbitMqSender.serviceRequestServiceRequest(serviceRequestResponse);
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
            Optional<LoanDto> r = queryService.queryLoanId(id);
            if ( r.isPresent() ) {
                return objectMapper.writeValueAsString(r.get());
            } else {
                return "ERROR: Loan not found for id: " + id;
            }
        } catch ( Exception ex) {
            log.error("String receivedQueryLoanIdMessage(UUID id) exception {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }
}