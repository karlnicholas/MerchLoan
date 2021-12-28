package com.github.karlnicholas.merchloan.servicerequest.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.karlnicholas.merchloan.apimessage.message.*;
import com.github.karlnicholas.merchloan.jms.message.RabbitMqSender;
import com.github.karlnicholas.merchloan.jmsmessage.CreateAccount;
import com.github.karlnicholas.merchloan.jmsmessage.CreditToLoan;
import com.github.karlnicholas.merchloan.jmsmessage.DebitFromLoan;
import com.github.karlnicholas.merchloan.jmsmessage.FundLoan;
import com.github.karlnicholas.merchloan.servicerequest.model.ServiceRequest;
import com.github.karlnicholas.merchloan.servicerequest.repository.ServiceRequestRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@Slf4j
public class ServiceRequestService {
    private final RabbitMqSender rabbitMqSender;
    private final ServiceRequestRepository serviceRequestRepository;
    private final ObjectMapper objectMapper;

    public ServiceRequestService(RabbitMqSender rabbitMqSender, ServiceRequestRepository serviceRequestRepository, ObjectMapper objectMapper) {
        this.rabbitMqSender = rabbitMqSender;
        this.serviceRequestRepository = serviceRequestRepository;
        this.objectMapper = objectMapper;
    }

    public UUID accountRequest(AccountRequest accountRequest) throws JsonProcessingException {
        UUID id = persistRequest(accountRequest);
        try {
            rabbitMqSender.sendCreateAccount(CreateAccount.builder()
                    .id(id)
                    .customer(accountRequest.getCustomer())
                    .createDate(LocalDate.now())
                    .build());
        } catch (Exception e) {
            log.error("Send account create message failed: {}", e.getMessage());
        }
        return id;
    }

    public UUID fundingRequest(FundingRequest fundingRequest) throws JsonProcessingException {
        UUID id = persistRequest(fundingRequest);
        rabbitMqSender.sendFundingRequest(
                FundLoan.builder()
                        .id(id)
                        .accountId(fundingRequest.getAccountId())
                        .lender(fundingRequest.getLender())
                        .amount(fundingRequest.getAmount())
                        .startDate(LocalDate.now())
                        .build()
        );
        return id;
    }

    public UUID creditRequest(CreditRequest creditRequest) throws JsonProcessingException {
        UUID id = persistRequest(creditRequest);
        rabbitMqSender.sendCreditRequest(
                CreditToLoan.builder()
                        .id(id)
                        .loanId(creditRequest.getLoanId())
                        .date(LocalDate.now())
                        .amount(creditRequest.getAmount())
                        .build()
        );
        return id;
    }

    public UUID debitRequest(DebitRequest debitRequest) throws JsonProcessingException {
        UUID id = persistRequest(debitRequest);
        rabbitMqSender.sendDebitRequest(
                DebitFromLoan.builder()
                        .loanId(debitRequest.getLoanId())
                        .date(LocalDate.now())
                        .amount(debitRequest.getAmount())
                        .build()
        );
        return id;
    }

    private UUID persistRequest(ServiceRequestMessage requestMessage) throws JsonProcessingException {
        UUID id = UUID.randomUUID();
        boolean retry;
        do {
            retry = false;
            try {
                serviceRequestRepository.save(
                        ServiceRequest.builder()
                                .id(id)
                                .request(objectMapper.writeValueAsString(requestMessage))
                                .localDateTime(LocalDateTime.now())
                                .requestType(requestMessage.getClass().getName())
                                .transacted(Boolean.FALSE)
                                .build()
                );
            } catch (DuplicateKeyException dke) {
                id = UUID.randomUUID();
                retry = true;
            }
        } while ( retry );
        return id;
    }
}
