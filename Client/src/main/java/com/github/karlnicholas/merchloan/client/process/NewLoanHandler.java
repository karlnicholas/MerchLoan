package com.github.karlnicholas.merchloan.client.process;

import com.github.karlnicholas.merchloan.client.component.AccountComponent;
import com.github.karlnicholas.merchloan.client.component.LoanComponent;
import com.github.karlnicholas.merchloan.client.component.LoanStateComponent;
import com.github.karlnicholas.merchloan.client.component.RequestStatusComponent;
import com.github.karlnicholas.merchloan.dto.LoanDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpException;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

@Component
@Slf4j
public class NewLoanHandler implements LoanProcessHandler {
    private final AccountComponent accountComponent;
    private final LoanComponent loanComponent;
    private final LoanStateComponent loanStateComponent;
    private final RequestStatusComponent requestStatusComponent;

    public NewLoanHandler(AccountComponent accountComponent, LoanComponent loanComponent, LoanStateComponent loanStateComponent, RequestStatusComponent requestStatusComponent) {
        this.accountComponent = accountComponent;
        this.loanComponent = loanComponent;
        this.loanStateComponent = loanStateComponent;
        this.requestStatusComponent = requestStatusComponent;
    }

    @Override
    public boolean progressState(LoanData loanData) throws ExecutionException, InterruptedException, HttpException, IOException {
        // Open Account
        Optional<UUID> accountId = accountComponent.createAccount(loanData.getCustomer());
        if ( accountId.isEmpty()) {
            return false;
        }
        Optional<UUID> requestId = requestStatusComponent.checkRequestStatus(accountId.get());
        if ( requestId.isEmpty()) {
            return false;
        }
        Optional<UUID> loanId = loanComponent.fundLoan(accountId.get(), loanData.getFundingAmount(), LoanData.FUNDING_DESCRIPTION);
        if ( loanId.isEmpty()) {
            return false;
        }
        loanData.setLoanId(loanId.get());
        requestId = requestStatusComponent.checkRequestStatus(loanId.get());
        if ( requestId.isEmpty()) {
            return false;
        }
        Optional<LoanDto> loanState = loanStateComponent.checkLoanStatus(loanId.get());
        if ( loanState.isEmpty()) {
            return false;
        }
        loanData.setLoanState(loanState.get());
        return true;
    }
}
