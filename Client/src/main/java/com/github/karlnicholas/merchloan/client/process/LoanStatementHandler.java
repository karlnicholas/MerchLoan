package com.github.karlnicholas.merchloan.client.process;

import com.github.karlnicholas.merchloan.client.component.LoanStateComponent;
import com.github.karlnicholas.merchloan.client.component.RequestStatusComponent;
import com.github.karlnicholas.merchloan.dto.LoanDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.UUID;

@Component
@Slf4j
public class LoanStatementHandler implements LoanProcessHandler {
    private final LoanStateComponent loanStateComponent;
    private final RequestStatusComponent requestStatusComponent;

    public LoanStatementHandler(LoanStateComponent loanStateComponent, RequestStatusComponent requestStatusComponent) {
        this.loanStateComponent = loanStateComponent;
        this.requestStatusComponent = requestStatusComponent;
    }

    @Override
    public boolean progressState(LoanData loanData) {
        if (!checkLastPaymentStatus(loanData)) {
            return false;
        }
        return getLoanState(loanData);
    }

    private boolean checkLastPaymentStatus(LoanData loanData) {
        // Check request status
        int requestCount = 0;
        boolean loop = true;
        int waitTime = 300;
        do {
            Optional<UUID> requestStatus = requestStatusComponent.checkRequestStatus(loanData.getLastPaymentRequestId());
            if (requestStatus.isPresent()) {
                return true;
            }
            requestCount++;
            if ( requestCount > 3 ) {
                loop = false;
            }
            if (loop) {
                sleep(waitTime);
                waitTime *= 3;
            }
        } while (loop);
        return false;
    }

    private boolean getLoanState(LoanData loanData) {
        // Check request status
        int requestCount = 0;
        boolean loop = true;
        int waitTime = 300;
        do {
            Optional<LoanDto> loanState = loanStateComponent.checkLoanStatus(loanData.getLoanId());
            if (loanState.isPresent()) {
                LoanDto loanDto = loanState.get();
                if (loanDto.getLoanState().equalsIgnoreCase("CLOSED") || ( loanDto.getLastStatementDate() != null && loanDto.getLastStatementDate().isEqual(loanData.getLastStatementDate()))) {
                    loanData.setLoanState(loanDto);
                    return true;
                }
            }
            requestCount++;
            if ( requestCount > 3 ) {
                loop = false;
            }
            if (loop) {
                sleep(waitTime);
                waitTime *= 3;
            }
        } while (loop);
        return false;
    }


    private void sleep(int waitTime) {
        try {
            Thread.sleep(waitTime);
        } catch (InterruptedException ex) {
            log.error("Sleep while check status interrupted:", ex);
            Thread.currentThread().interrupt();
        }
    }
}
