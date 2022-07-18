package com.github.karlnicholas.merchloan.servicerequest.api;

import com.github.karlnicholas.merchloan.apimessage.message.*;
import com.github.karlnicholas.merchloan.servicerequest.component.ServiceRequestRouter;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("api")
@RequestScoped
public class ServiceRequestController {
    private final ServiceRequestRouter serviceRequestRouter;

    @Inject
    public ServiceRequestController(ServiceRequestRouter serviceRequestRouter) {
        this.serviceRequestRouter = serviceRequestRouter;
    }
    @POST
    @Path("accountRequest")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String createAccountRequest(@BeanParam AccountRequest accountRequest) throws Exception {
        log.debug("accountRequest: {}", accountRequest);
        return serviceRequestRouter.routeRequest(accountRequest.getClass().getName(), accountRequest, Boolean.FALSE, null).toString();
    }
    @Path("fundingRequest")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String fundingRequest(@BeanParam FundingRequest fundingRequest) throws Exception {
        log.debug("fundingRequest: {}", fundingRequest);
        return serviceRequestRouter.routeRequest(fundingRequest.getClass().getName(), fundingRequest, Boolean.FALSE, null).toString();
    }
    @Path("creditRequest")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String creditRequest(@BeanParam CreditRequest creditRequest) throws Exception {
        log.debug("creditRequest: {}", creditRequest);
        return serviceRequestRouter.routeRequest(creditRequest.getClass().getName(), creditRequest, Boolean.FALSE, null).toString();
    }
    @Path("debitRequest")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String debitRequest(@BeanParam DebitRequest debitRequest) throws Exception {
        log.debug("debitRequest: {}", debitRequest);
        return serviceRequestRouter.routeRequest(debitRequest.getClass().getName(), debitRequest, Boolean.FALSE, null).toString();
    }
    @Path("statementRequest")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String statementRequest(@BeanParam StatementRequest statementRequest) throws Exception {
        log.debug("statementRequest: {}", statementRequest);
        return serviceRequestRouter.routeRequest(statementRequest.getClass().getName(), statementRequest, Boolean.FALSE, null).toString();
    }
    @Path("closeRequest")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.TEXT_PLAIN)
    public String fundingRequest(@BeanParam CloseRequest closeRequest) throws Exception {
        log.debug("closeRequest: {}", closeRequest);
        return serviceRequestRouter.routeRequest(closeRequest.getClass().getName(), closeRequest, Boolean.FALSE, null).toString();
    }
}