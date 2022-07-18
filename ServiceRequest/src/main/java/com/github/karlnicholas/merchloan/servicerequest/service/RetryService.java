package com.github.karlnicholas.merchloan.servicerequest.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.karlnicholas.merchloan.apimessage.message.ServiceRequestMessage;
import com.github.karlnicholas.merchloan.servicerequest.component.ServiceRequestRouter;
import com.github.karlnicholas.merchloan.servicerequest.model.ServiceRequest;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetryService {
    private final ServiceRequestRouter serviceRequestRouter;
    private final ObjectMapper objectMapper;

    public RetryService(ServiceRequestRouter serviceRequestRouter, ObjectMapper objectMapper) {
        this.serviceRequestRouter = serviceRequestRouter;
        this.objectMapper = objectMapper;
    }

    public void retryServiceRequest(ServiceRequest serviceRequest, String requestType)  {
        try {
            Class<? extends ServiceRequestMessage> messageClass = (Class<? extends ServiceRequestMessage>) Class.forName(requestType);
            ServiceRequestMessage serviceRequestMessage = objectMapper.readValue(serviceRequest.getRequest(), messageClass);
            log.debug("retryServiceRequest {}", serviceRequestMessage);
            serviceRequestRouter.routeRequest(requestType, serviceRequestMessage, Boolean.TRUE, serviceRequest.getId());
        } catch (Exception e) {
            log.error("ERROR in RETRY", e);
        }
    }
}
