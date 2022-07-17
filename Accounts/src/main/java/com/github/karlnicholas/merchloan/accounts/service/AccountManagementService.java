package com.github.karlnicholas.merchloan.accounts.service;

import com.github.karlnicholas.merchloan.accounts.dao.AccountDao;
import com.github.karlnicholas.merchloan.accounts.dao.LoanDao;
import com.github.karlnicholas.merchloan.accounts.model.Account;
import com.github.karlnicholas.merchloan.accounts.model.Loan;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.github.karlnicholas.merchloan.sqlutil.SqlUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.*;

@Service
@Slf4j
public class AccountManagementService {
    private final DataSource dataSource;
    private final AccountDao accountDao;
    private final LoanDao loanDao;

    @Autowired
    public AccountManagementService(DataSource dataSource, AccountDao accountDao, LoanDao loanDao) {
        this.dataSource = dataSource;
        this.accountDao = accountDao;
        this.loanDao = loanDao;
    }

    public void createAccount(CreateAccount createAccount, ServiceRequestResponse requestResponse) {
        try (Connection con = dataSource.getConnection()) {
            accountDao.createAccount(con, Account.builder()
                    .id(createAccount.getId())
                    .customer(createAccount.getCustomer())
                    .createDate(createAccount.getCreateDate())
                    .build());
            requestResponse.setSuccess();
        } catch (SQLException ex) {
            if (ex.getErrorCode() == SqlUtils.DUPLICATE_ERROR && Boolean.TRUE.equals(createAccount.getRetry())) {
                requestResponse.setSuccess();
            } else {
                requestResponse.setError(ex.getMessage());
            }
            log.error("createAccount {}", ex);
        } catch (Exception ex) {
            log.error("createAccount {}", ex);
            requestResponse.setError(ex.getMessage());
        }
    }

    public void fundAccount(FundLoan fundLoan, ServiceRequestResponse requestResponse) {
        try (Connection con = dataSource.getConnection()) {
            Optional<Account> accountQ = accountDao.findById(con, fundLoan.getAccountId());
            if (accountQ.isPresent()) {
                LocalDate[] statementDates = new LocalDate[12];
                statementDates[11] = fundLoan.getStartDate().plusYears(1);
                for (int i = 0; i < 11; ++i) {
                    statementDates[i] = fundLoan.getStartDate().plusDays((int) ((i + 1) * 365.0 / 12.0));
                }
                loanDao.insert(con,
                        Loan.builder()
                                .id(fundLoan.getId())
                                .accountId(accountQ.get().getId())
                                .startDate(fundLoan.getStartDate())
                                .statementDates(Arrays.asList(statementDates))
                                .funding(fundLoan.getAmount())
                                .months(12)
                                .interestRate(new BigDecimal("0.10"))
                                .monthlyPayments(new BigDecimal("879.16"))
                                .loanState(Loan.LOAN_STATE.OPEN)
                                .build());
                requestResponse.setSuccess();
            } else {
                requestResponse.setError("Account not found for " + fundLoan.getAccountId());
            }
        } catch (SQLException ex) {
            if (ex.getErrorCode() == SqlUtils.DUPLICATE_ERROR && Boolean.TRUE.equals(fundLoan.getRetry())) {
                requestResponse.setSuccess();
            } else {
                requestResponse.setError(ex.getMessage());
            }
            log.error("fundAccount {}", ex);
        } catch (Exception ex) {
            log.error("fundAccount {}", ex);
            requestResponse.setError(ex.getMessage());
        }
    }

    public void validateLoan(UUID loanId, ServiceRequestResponse requestResponse) {
        try (Connection con = dataSource.getConnection()) {
            Optional<Loan> loanQ = loanDao.findById(con, loanId);
            if (loanQ.isPresent()) {
                requestResponse.setSuccess();
            } else {
                requestResponse.setError("Loan not found for " + loanId);
            }
        } catch (Exception ex) {
            log.error("validateLoan {}", ex);
            requestResponse.setError(ex.getMessage());
        }
    }

    public ServiceRequestResponse statementHeader(StatementHeader statementHeader) {
        ServiceRequestResponse requestResponse = ServiceRequestResponse.builder()
                .id(statementHeader.getId())
                .build();
        try (Connection con = dataSource.getConnection()) {
            log.debug("statementHeader con {}", statementHeader.getLoanId());
            Optional<Loan> loanOpt = loanDao.findById(con, statementHeader.getLoanId());
            log.debug("statementHeader loanOpt: {} {}", loanOpt, statementHeader.getLoanId());
            if (loanOpt.isPresent()) {
                Optional<Account> accountQ = accountDao.findById(con, loanOpt.get().getAccountId());
                log.debug("statementHeader accountQ: {} {}", accountQ, statementHeader.getLoanId());
                if (accountQ.isPresent()) {
                    statementHeader.setCustomer(accountQ.get().getCustomer());
                    statementHeader.setAccountId(loanOpt.get().getAccountId());
                    statementHeader.setLoanFunding(loanOpt.get().getFunding());
                    requestResponse.setSuccess();
                } else {
                    requestResponse.setError("Account not found for loanId: " + statementHeader.getLoanId());
                }
            } else {
                requestResponse.setError("Loan not found for loanId: " + statementHeader.getLoanId());
            }
        } catch (SQLException ex) {
            if (ex.getErrorCode() == SqlUtils.DUPLICATE_ERROR && Boolean.TRUE.equals(statementHeader.getRetry())) {
                requestResponse.setSuccess();
            } else {
                requestResponse.setError(ex.getMessage());
            }
            log.error("statementHeader {}", ex);
        } catch (Exception ex) {
            log.error("statementHeader {}", ex);
            requestResponse.setError(ex.getMessage());
        }
        log.debug("statementHeader requestResponse: {}", statementHeader.getLoanId());
        return requestResponse;
    }

    public List<BillingCycle> loansToCycle(LocalDate businessDate) {
        try (Connection con = dataSource.getConnection()) {
            List<Loan> loans = loanDao.findByLoanState(con, Loan.LOAN_STATE.OPEN);
            ArrayList<BillingCycle> loansToCycle = new ArrayList<>();
            loans.forEach(loan -> {
                List<LocalDate> statementDates = loan.getStatementDates();
                int i = Collections.binarySearch(statementDates, businessDate);
                if (i >= 0) {
                    loansToCycle.add(BillingCycle.builder()
                            .loanId(loan.getId())
                            .accountId(loan.getAccountId())
                            .statementDate(businessDate)
                            .startDate(i == 0 ? loan.getStartDate() : statementDates.get(i - 1).plusDays(1))
                            .endDate(businessDate)
                            .build());
                }
            });
            return loansToCycle;
        } catch (Exception ex) {
            log.error("loansToCycle {}", ex);
            return Collections.emptyList();
        }
    }

    public void closeLoan(UUID loanId) {
        try (Connection con = dataSource.getConnection()) {
            loanDao.updateState(con, loanId, Loan.LOAN_STATE.CLOSED);
        } catch (Exception ex) {
            log.error("closeLoan {}", ex);
        }
    }
}
