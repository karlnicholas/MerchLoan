package com.github.karlnicholas.merchloan.accounts.service;

import com.github.karlnicholas.merchloan.accounts.model.Account;
import com.github.karlnicholas.merchloan.accounts.model.Loan;
import com.github.karlnicholas.merchloan.accounts.repository.AccountRepository;
import com.github.karlnicholas.merchloan.accounts.repository.LoanRepository;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class AccountManagementService {
    private final AccountRepository accountRepository;
    private final LoanRepository loanRepository;

    @Autowired
    public AccountManagementService(AccountRepository accountRepository, LoanRepository loanRepository) {
        this.accountRepository = accountRepository;
        this.loanRepository = loanRepository;
    }

    public void createAccount(CreateAccount createAccount, ServiceRequestResponse requestResponse) {
        try {
            accountRepository.save(Account.builder()
                    .id(createAccount.getId())
                    .customer(createAccount.getCustomer())
                    .createDate(createAccount.getCreateDate())
                    .build()
            );
            requestResponse.setSuccess("Account created");
        } catch (DuplicateKeyException dke) {
            log.warn("Create Account duplicate key exception: {}", dke.getMessage());
            if (createAccount.getRetry().booleanValue()) {
                requestResponse.setSuccess("Account created");
            } else {
                requestResponse.setFailure(dke.getMessage());
            }
        }
    }

    public void fundAccount(FundLoan fundLoan, ServiceRequestResponse requestResponse) {
        Optional<Account> accountQ = accountRepository.findById(fundLoan.getAccountId());
        if (accountQ.isPresent()) {
            try {
                LocalDate[] statementDates = new LocalDate[12];
                statementDates[11] = fundLoan.getStartDate().plusYears(1);
                for ( int i = 0; i < 11; ++i ) {
                    statementDates[i] = fundLoan.getStartDate().plusDays((int)((i+1)*365.0/12.0));
                }

                loanRepository.save(
                        Loan.builder()
                                .id(fundLoan.getId())
                                .account(accountQ.get())
                                .startDate(fundLoan.getStartDate())
                                .statementDates(Arrays.asList(statementDates))
                                .funding(fundLoan.getAmount())
                                .months(12)
                                .interestRate(new BigDecimal("0.10"))
                                .monthlyPayments(new BigDecimal("879.16"))
                                .loanState(Loan.LOAN_STATE.OPEN)
                                .build());
                requestResponse.setSuccess();
            } catch (DuplicateKeyException dke) {
                log.warn("Create Account duplicate key exception: {}", dke.getMessage());
                if (!fundLoan.getRetry().booleanValue()) {
                    requestResponse.setFailure(dke.getMessage());
                }
            }
        } else {
            requestResponse.setFailure("Account not found for " + fundLoan.getAccountId());
        }
    }

    public void validateLoan(UUID loanId, ServiceRequestResponse requestResponse) {
        Optional<Loan> loanQ = loanRepository.findById(loanId);
        if (loanQ.isPresent()) {
            requestResponse.setSuccess();
        } else {
            requestResponse.setFailure("Loan not found for " + loanId);
        }
    }

    public ServiceRequestResponse statementHeader(StatementHeader statementHeader) {
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(statementHeader.getId())
                .build();
        Optional<Loan> loanOpt = loanRepository.findById(statementHeader.getLoanId());
        if (loanOpt.isPresent()) {
            Optional<Account> accountQ = accountRepository.findById(loanOpt.get().getAccount().getId());
            if (accountQ.isPresent()) {
                statementHeader.setCustomer(accountQ.get().getCustomer());
                statementHeader.setAccountId(loanOpt.get().getAccount().getId());
                requestResponse.setSuccess();
            } else {
                requestResponse.setFailure("Account not found for loanId: " + statementHeader.getLoanId());
            }
        } else {
            requestResponse.setFailure("Loan not found for loanId: " + statementHeader.getLoanId());
        }
        return requestResponse;
    }

    public List<BillingCycle> loansToCycle(LocalDate businessDate) {
        return loanRepository.findByLoanState(Loan.LOAN_STATE.OPEN)
                .stream()
                .filter(l -> Collections.binarySearch(l.getStatementDates(), businessDate) >= 0)
                .map(l -> {
                    List<LocalDate> statementDates = l.getStatementDates();
                    int i = Collections.binarySearch(statementDates, businessDate);
                    return BillingCycle.builder()
                            .accountId(l.getAccount().getId())
                            .loanId(l.getId())
                            .statementDate(businessDate)
                            .startDate(i == 0 ? l.getStartDate() : statementDates.get(i - 1).plusDays(1))
                            .endDate(businessDate)
                            .build();
                })
                .collect(Collectors.toList());
    }

    public void closeLoan(UUID loanId) {
        loanRepository.findById(loanId).ifPresent(loan -> {
            loan.setLoanState(Loan.LOAN_STATE.CLOSED);
            loanRepository.save(loan);
        });
    }

}
