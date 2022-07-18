package com.github.karlnicholas.merchloan.accounts.dao;

import com.github.karlnicholas.merchloan.accounts.model.Loan;
import com.github.karlnicholas.merchloan.accounts.model.StatementDatesConverter;
import com.github.karlnicholas.merchloan.sqlutil.SqlUtils;
import jakarta.enterprise.context.ApplicationScoped;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@ApplicationScoped
public class LoanDao {
    //id, account_id, start_date, statement_dates, funding, months, interest_rate, monthly_payments, loan_state
    public void insert(Connection con, Loan loan) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("insert into loan(id, account_id, start_date, statement_dates, funding, months, interest_rate, monthly_payments, loan_state) values(?, ?, ?, ?, ?, ?, ?, ?, ?)" )) {
            ps.setBytes(1, SqlUtils.uuidToBytes(loan.getId()));
            ps.setBytes(2, SqlUtils.uuidToBytes(loan.getAccountId()));
            ps.setDate(3, java.sql.Date.valueOf(loan.getStartDate()));
            ps.setString(4, StatementDatesConverter.convertToDatabaseColumn(loan.getStatementDates()));
            ps.setBigDecimal(5, loan.getFunding());
            ps.setInt(6, loan.getMonths());
            ps.setBigDecimal(7, loan.getInterestRate());
            ps.setBigDecimal(8, loan.getMonthlyPayments());
            ps.setInt(9, loan.getLoanState().ordinal());
            ps.executeUpdate();
        }
    }

    public Optional<Loan> findById(Connection con, UUID loanId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select id, account_id, start_date, statement_dates, funding, months, interest_rate, monthly_payments, loan_state from loan where id = ?")) {
            ps.setBytes(1, SqlUtils.uuidToBytes(loanId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return Optional.of(Loan.builder()
                            .id(SqlUtils.toUUID(rs.getBytes(1)))
                            .accountId(SqlUtils.toUUID(rs.getBytes(2)))
                            .startDate(((Date) rs.getObject(3)).toLocalDate())
                            .statementDates(StatementDatesConverter.convertToEntityAttribute(rs.getString(4)))
                            .funding(rs.getBigDecimal(5))
                            .months(rs.getInt(6))
                            .interestRate(rs.getBigDecimal(7))
                            .monthlyPayments(rs.getBigDecimal(8))
                            .loanState(Loan.LOAN_STATE.values()[rs.getInt(9)])
                            .build());
                else
                    return Optional.empty();
            }
        }
    }

    public List<Loan> findByLoanState(Connection con, Loan.LOAN_STATE state) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select id, account_id, start_date, statement_dates, funding, months, interest_rate, monthly_payments, loan_state from loan where loan_state = ?")) {
            ps.setInt(1, state.ordinal());
            try (ResultSet rs = ps.executeQuery()) {
                List<Loan> loans = new ArrayList<>();
                while (rs.next()) {
                    loans.add(Loan.builder()
                            .id(SqlUtils.toUUID(rs.getBytes(1)))
                            .accountId(SqlUtils.toUUID(rs.getBytes(2)))
                            .startDate(((Date) rs.getObject(3)).toLocalDate())
                            .statementDates(StatementDatesConverter.convertToEntityAttribute(rs.getString(4)))
                            .funding(rs.getBigDecimal(5))
                            .months(rs.getInt(6))
                            .interestRate(rs.getBigDecimal(7))
                            .monthlyPayments(rs.getBigDecimal(8))
                            .loanState(Loan.LOAN_STATE.values()[rs.getInt(9)])
                            .build());
                }
                return loans;
            }
        }
    }

    public void updateState(Connection con, UUID loanId, Loan.LOAN_STATE state) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("update loan set loan_state = ? where id = ?" )) {
            ps.setInt(1, state.ordinal());
            ps.setBytes(2, SqlUtils.uuidToBytes(loanId));
            ps.executeUpdate();
        }
    }
}
