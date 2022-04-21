package com.github.karlnicholas.merchloan.accounts.service;

import com.github.karlnicholas.merchloan.accounts.dao.RegisterEntryDao;
import com.github.karlnicholas.merchloan.accounts.model.RegisterEntry;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@Slf4j
@Transactional
public class RegisterManagementService {
    private final DataSource dataSource;
    private final RegisterEntryDao registerEntryDao;

    public RegisterManagementService(DataSource dataSource, RegisterEntryDao registerEntryDao) {
        this.dataSource = dataSource;
        this.registerEntryDao = registerEntryDao;
    }

    public void fundLoan(DebitLoan fundLoan, ServiceRequestResponse requestResponse) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            registerEntryDao.insert(con, RegisterEntry.builder()
                    .id(fundLoan.getId())
                    .loanId(fundLoan.getLoanId())
                    .date(fundLoan.getDate())
                    .debit(fundLoan.getAmount())
                    .description(fundLoan.getDescription())
                    .build());
            requestResponse.setSuccess();
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 2601 && fundLoan.getRetry() == Boolean.TRUE) {
                requestResponse.setSuccess();
            }
            log.error("fundLoan {}", ex);
            requestResponse.setFailure(ex.getMessage());
        } catch (Exception ex) {
            log.error("fundLoan {}", ex);
            requestResponse.setFailure(ex.getMessage());
        }
    }

    public void debitLoan(DebitLoan debitLoan, ServiceRequestResponse requestResponse) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            RegisterEntry debitEntry = RegisterEntry.builder()
                    .id(debitLoan.getId())
                    .loanId(debitLoan.getLoanId())
                    .date(debitLoan.getDate())
                    .debit(debitLoan.getAmount())
                    .description(debitLoan.getDescription())
                    .build();
            registerEntryDao.insert(con, debitEntry);
            requestResponse.setSuccess();
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 2601 && debitLoan.getRetry() == Boolean.TRUE) {
                requestResponse.setSuccess();
            }
            log.error("debitLoan {}", ex);
            requestResponse.setFailure(ex.getMessage());
        } catch (Exception ex) {
            log.error("debitLoan {}", ex);
            requestResponse.setFailure(ex.getMessage());
        }
    }

    public void creditLoan(CreditLoan creditLoan, ServiceRequestResponse requestResponse) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            RegisterEntry creditEntry = RegisterEntry.builder()
                    .id(creditLoan.getId())
                    .loanId(creditLoan.getLoanId())
                    .date(creditLoan.getDate())
                    .credit(creditLoan.getAmount())
                    .description(creditLoan.getDescription())
                    .build();
            registerEntryDao.insert(con, creditEntry);
            requestResponse.setSuccess();
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 2601 && creditLoan.getRetry() == Boolean.TRUE) {
                requestResponse.setSuccess();
            }
            log.error("creditLoan {}", ex);
            requestResponse.setFailure(ex.getMessage());
        } catch (Exception ex) {
            log.error("creditLoan {}", ex);
            requestResponse.setFailure(ex.getMessage());
        }
    }

    public void setStatementHeaderRegisterEntryies(StatementHeader statementHeader) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            statementHeader.setRegisterEntries(registerEntryDao.findByLoanIdAndDateBetweenOrderByTimestamp(con, statementHeader.getLoanId(), statementHeader.getStartDate(), statementHeader.getEndDate())
                    .stream().map(e -> RegisterEntryMessage.builder()
                            .date(e.getDate())
                            .credit(e.getCredit())
                            .debit(e.getDebit())
                            .description(e.getDescription())
                            .timeStamp(e.getTimeStamp())
                            .build())
                    .collect(Collectors.toList()));
        }
    }

    public RegisterEntry billingCycleCharge(BillingCycleCharge billingCycleCharge) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            if (billingCycleCharge.getRetry() == Boolean.TRUE) {
                Optional<RegisterEntry> reOpt = registerEntryDao.findById(con, billingCycleCharge.getId());
                if (reOpt.isPresent()) {
                    return reOpt.get();
                }
            }
            RegisterEntry registerEntry = RegisterEntry.builder()
                    .id(billingCycleCharge.getId())
                    .loanId(billingCycleCharge.getLoanId())
                    .date(billingCycleCharge.getDate())
                    .debit(billingCycleCharge.getDebit())
                    .credit(billingCycleCharge.getCredit())
                    .description(billingCycleCharge.getDescription())
                    .build();
            registerEntryDao.insert(con, registerEntry);
            return registerEntry;
        }
    }
}
