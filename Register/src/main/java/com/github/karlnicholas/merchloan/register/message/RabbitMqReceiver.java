package com.github.karlnicholas.merchloan.register.message;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.karlnicholas.merchloan.apimessage.message.ServiceRequestMessage;
import com.github.karlnicholas.merchloan.jms.message.RabbitMqSender;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.register.service.RegisterManagementService;
import com.github.karlnicholas.merchloan.register.service.QueryService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpRejectAndDontRequeueException;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.annotation.RabbitListenerConfigurer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistrar;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@Slf4j
public class RabbitMqReceiver implements RabbitListenerConfigurer {
    private final RegisterManagementService registerManagementService;
    private final QueryService queryService;
    private final ObjectMapper objectMapper;
    private final RabbitMqSender rabbitMqSender;

    public RabbitMqReceiver(RegisterManagementService registerManagementService, QueryService queryService, RabbitMqSender rabbitMqSender) {
        this.registerManagementService = registerManagementService;
        this.queryService = queryService;
        this.rabbitMqSender = rabbitMqSender;
        this.objectMapper = new ObjectMapper().findAndRegisterModules()
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    }

    @Override
    public void configureRabbitListeners(RabbitListenerEndpointRegistrar rabbitListenerEndpointRegistrar) {
    }

    @RabbitListener(queues = "${rabbitmq.register.fundloan.queue}")
    public void receivedFundLoanMessage(DebitLoan debitLoan) {
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(debitLoan.getId())
                .build();
        try {
            log.info("FundLoan Received {}", debitLoan);
            registerManagementService.fundLoan(debitLoan, requestResponse);
        } catch (Exception ex) {
            log.error("void receivedCreditLoanMessage(CreditLoan creditLoan) exception {}", ex.getMessage());
            requestResponse.setError(ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        } finally {
            rabbitMqSender.serviceRequestServiceRequest(requestResponse);
        }
    }

    @RabbitListener(queues = "${rabbitmq.register.creditloan.queue}")
    public void receivedCreditLoanMessage(CreditLoan creditLoan) {
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(creditLoan.getId())
                .build();
        try {
            log.info("CreditLoan Received {}", creditLoan);
            registerManagementService.creditLoan(creditLoan, requestResponse);
        } catch (Exception ex) {
            log.error("void receivedCreditLoanMessage(CreditLoan creditLoan) exception {}", ex.getMessage());
            requestResponse.setError(ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        } finally {
            rabbitMqSender.serviceRequestServiceRequest(requestResponse);
        }
    }

    @RabbitListener(queues = "${rabbitmq.register.debitloan.queue}")
    public void receivedDebitLoanMessage(DebitLoan debitLoan) {
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(debitLoan.getId())
                .build();
        try {
            log.info("DebitLoan Received {}", debitLoan);
            registerManagementService.debitLoan(debitLoan, requestResponse);
        } catch (Exception ex) {
            log.error("void receivedDebitLoanMessage(DebitLoan debitLoan) exception {}", ex.getMessage());
            requestResponse.setError(ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        } finally {
            rabbitMqSender.serviceRequestServiceRequest(requestResponse);
        }
    }

    @RabbitListener(queues = "${rabbitmq.register.statementheader.queue}")
    public StatementHeader receivedStatementHeaderMessage(StatementHeader statementHeader) {
        try {
            log.info("StatementHeader Received {}", statementHeader);
            registerManagementService.statementHeader(statementHeader);
            return statementHeader;
        } catch (Exception ex) {
            log.error("String receivedQueryLoanIdMessage(UUID id) exception {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.register.billingcyclecharge.queue}")
    public void receivedBillingCycleChargeMessage(BillingCycleCharge billingCycleCharge) {
        try {
            log.info("BillingCycleCharge Received {}", billingCycleCharge);
            registerManagementService.billingCycleCharge(billingCycleCharge);
        } catch (Exception ex) {
            log.error("void receivedDebitLoanMessage(DebitLoan debitLoan) exception {}", ex.getMessage());
            billingCycleCharge.setError(ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        } finally {
            rabbitMqSender.serviceRequestChargeCompleted(billingCycleCharge);
        }
    }

    @RabbitListener(queues = "${rabbitmq.register.closeloan.queue}")
    public void receivedCloseLoanMessage(CloseLoan closeLoan) {
        try {
            log.info("CloseLoan Received {}", closeLoan);
            StatementHeader statementHeader = StatementHeader.builder()
                    .id(closeLoan.getId())
                    .accountId(closeLoan.getLoanDto().getAccountId())
                    .loanId(closeLoan.getLoanId())
                    .statementDate(closeLoan.getDate())
                    .startDate(closeLoan.getLastStatementDate().plusDays(1))
                    .endDate(closeLoan.getDate())
                    .build();
            registerManagementService.statementHeader(statementHeader);
            rabbitMqSender.statementCloseStatement(statementHeader);
        } catch (Exception ex) {
            log.error("void receivedDebitLoanMessage(DebitLoan debitLoan) exception {}", ex.getMessage());
            try {
                ServiceRequestResponse requestResponse = new ServiceRequestResponse(closeLoan.getId(), ServiceRequestMessage.STATUS.ERROR, ex.getMessage());
                rabbitMqSender.serviceRequestServiceRequest(requestResponse);
            } catch ( Exception innerEx ) {
                log.error("ERROR SENDING ERROR", ex);
            }
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }

    @RabbitListener(queues = "${rabbitmq.register.query.loan.id.queue}")
    public String receivedQueryLoanIdMessage(UUID id) {
        try {
            log.info("QueryLoanId Received {}", id);
            return objectMapper.writeValueAsString(queryService.queryRegisterByLoanId(id));
        } catch (Exception ex) {
            log.error("String receivedQueryLoanIdMessage(UUID id) exception {}", ex.getMessage());
            throw new AmqpRejectAndDontRequeueException(ex);
        }
    }
}