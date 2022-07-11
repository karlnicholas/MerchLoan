package com.github.karlnicholas.merchloan.client.process;

import com.github.karlnicholas.merchloan.client.component.*;
import com.github.karlnicholas.merchloan.client.rest.LoanProcessQueue;
import com.github.karlnicholas.merchloan.dto.LoanDto;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Optional;
import java.util.concurrent.Future;


@Slf4j
public class LoanCycle {
    enum CYCLE_STATES {NEW, PAYMENT, STATEMENT}

    private final LoanProcessHandler newLoanHandler;
    private final LoanProcessHandler paymentLoanHandler;
    private final LoanProcessHandler loanStatementHandler;
    private final LoanStateComponent loanStateComponent;
    private LoanProcessHandler currentLoanHandler;
    private LocalDate cycleDate;
    private CYCLE_STATES cycleState;
    private final LoanData loanData;
    private int statementIndex;
    private Future<Boolean> futureResult;

    public LoanCycle(CreditComponent creditComponent, AccountComponent accountComponent, LoanComponent loanComponent, LoanStateComponent loanStateComponent, RequestStatusComponent requestStatusComponent, LocalDate startDate, String customer) {
        cycleDate = startDate;
        this.loanStateComponent = loanStateComponent;
        newLoanHandler = new NewLoanHandler(accountComponent, loanComponent, loanStateComponent, requestStatusComponent);
        paymentLoanHandler = new LoanPaymentHandler(creditComponent);
        loanStatementHandler = new LoanStatementHandler(loanStateComponent, requestStatusComponent);
        currentLoanHandler = newLoanHandler;
        cycleState = CYCLE_STATES.NEW;
        statementIndex = 0;
        loanData = LoanData.builder()
                .customer(customer)
                .fundingAmount(BigDecimal.valueOf(10000.00))
                .build();
    }

    public void showStatement(String comment) {
        Optional<LoanDto> loanDto = loanStateComponent.checkLoanStatus(loanData.getLoanId());
        if (loanDto.isPresent())
            log.info("{}{ {}", comment, loanDto.get());
    }

    public boolean checkClosed() {
        return loanData.getLoanState().getLoanState().equalsIgnoreCase("CLOSED");
    }

    public void cycle(LoanProcessQueue loanProcessQueue, LocalDate currentDate) {
        if (cycleDate == null)
            return;
        try {
            if (currentDate != null && cycleDate.compareTo(currentDate) <= 0) {
                futureResult = loanProcessQueue.process(currentLoanHandler, loanData);
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
    public void updateState(LocalDate currentDate) {
        if (cycleDate == null)
            return;
        try {
            if (currentDate != null && cycleDate.compareTo(currentDate) <= 0) {
                LocalDate saveDate = cycleDate;
                CYCLE_STATES saveState = cycleState;
                boolean success = futureResult.get();
                if (success) {
                    if (cycleState == CYCLE_STATES.NEW) {
                        changeToPaymentOrComplete(currentDate);
                    } else if (cycleState == CYCLE_STATES.PAYMENT) {
                        changeToStatement();
                    } else if (cycleState == CYCLE_STATES.STATEMENT) {
                        changeToPaymentOrComplete(currentDate);
                    }
                } else {
                    log.error("Loan Cycle failed: {} {} {}", currentDate, cycleState, loanData.getLoanId());
                    cycleDate = null;
                }
                log.debug("{} {}=>{} {}=>{} {} LAST: {} STATE: {} LAST_STATEMENT_DATE: {}", success, saveDate, cycleDate, saveState, cycleState, loanData.getLoanId(), loanData.getLastPaymentRequestId(), loanData.getLoanState().getLoanState(), loanData.getLastStatementDate());
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }

    private void changeToStatement() {
        cycleDate = loanData.getLoanState().getStatementDates().get(statementIndex).plusDays(4);
        loanData.setLastStatementDate(loanData.getLoanState().getStatementDates().get(statementIndex++));
        cycleState = CYCLE_STATES.STATEMENT;
        currentLoanHandler = loanStatementHandler;
    }

    private void changeToPaymentOrComplete(LocalDate currentDate) {
        if (loanData.getLoanState().getLoanState().equalsIgnoreCase("CLOSED")) {
            cycleDate = null;
        } else if (loanData.getLoanState().getCurrentPayment().compareTo(loanData.getLoanState().getMonthlyPayments()) > 0) {
            log.error("Payment Error: {} {}", currentDate, loanData);
            cycleDate = null;
        } else {
            currentLoanHandler = paymentLoanHandler;
            cycleState = CYCLE_STATES.PAYMENT;
            cycleDate = loanData.getLoanState().getStatementDates().get(statementIndex).minusDays(5);
        }
    }

}
