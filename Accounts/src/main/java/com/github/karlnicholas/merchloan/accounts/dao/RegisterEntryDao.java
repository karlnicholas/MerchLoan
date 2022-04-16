package com.github.karlnicholas.merchloan.accounts.dao;

import com.github.karlnicholas.merchloan.accounts.model.RegisterEntry;
import com.github.karlnicholas.merchloan.sqlutil.UUIDToBytes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.time.LocalDate;
import java.util.*;

@Service
@Slf4j
public class RegisterEntryDao {
    // id, loan_id, row_num, date, description, debit, credit
    public Optional<RegisterEntry> findById(Connection con, UUID loanId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select id, loan_id, date, description, debit, credit, time_stamp from register_entry where id = ?")) {
            ps.setBytes(1, UUIDToBytes.uuidToBytes(loanId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return Optional.of(RegisterEntry.builder()
                            .id(UUIDToBytes.toUUID(rs.getBytes(1)))
                            .loanId(UUIDToBytes.toUUID(rs.getBytes(2)))
                            .date(rs.getDate(3).toLocalDate())
                            .description(rs.getString(4))
                            .debit(rs.getBigDecimal(5))
                            .credit(rs.getBigDecimal(6))
                            .timeStamp(rs.getTimestamp(7).toLocalDateTime())
                            .build());
                else
                    return Optional.empty();
            }
        }
    }

    // id, credit, date, debit, description , loan_id , row_num
    public List<RegisterEntry> findByLoanIdAndDateBetweenOrderByTimestamp(Connection con, UUID loanId, LocalDate startDate, LocalDate endDate) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select id, loan_id, date, description, debit, credit, time_stamp from register_entry where loan_id = ? and date between ? and ? order by time_stamp")) {
            ps.setBytes(1, UUIDToBytes.uuidToBytes(loanId));
            ps.setDate(2, java.sql.Date.valueOf(startDate));
            ps.setDate(3, java.sql.Date.valueOf(endDate));
            try (ResultSet rs = ps.executeQuery()) {
                List<RegisterEntry> registerEntries = new ArrayList<>();
                while (rs.next()) {
                    registerEntries.add(RegisterEntry.builder()
                            .id(UUIDToBytes.toUUID(rs.getBytes(1)))
                            .loanId(UUIDToBytes.toUUID(rs.getBytes(2)))
                            .date(rs.getDate(3).toLocalDate())
                            .description(rs.getString(4))
                            .debit(rs.getBigDecimal(5))
                            .credit(rs.getBigDecimal(6))
                            .timeStamp(rs.getTimestamp(7).toLocalDateTime())
                            .build());
                }
                return registerEntries;
            }
        }
    }

    public List<RegisterEntry> findByLoanIdOrderByRowNum(Connection con, UUID loanId) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select id, loan_id, date, description, debit, credit, time_stamp from register_entry where loan_id = ? order by time_stamp")) {
            ps.setBytes(1, UUIDToBytes.uuidToBytes(loanId));
            try (ResultSet rs = ps.executeQuery()) {
                List<RegisterEntry> registerEntries = new ArrayList<>();
                while (rs.next()) {
                    registerEntries.add(RegisterEntry.builder()
                            .id(UUIDToBytes.toUUID(rs.getBytes(1)))
                            .loanId(UUIDToBytes.toUUID(rs.getBytes(2)))
                            .date(rs.getDate(3).toLocalDate())
                            .description(rs.getString(4))
                            .debit(rs.getBigDecimal(5))
                            .credit(rs.getBigDecimal(6))
                            .timeStamp(rs.getTimestamp(7).toLocalDateTime())
                            .build());
                }
                return registerEntries;
            }
        }
    }

    public void insert(Connection con, RegisterEntry registerEntry) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("insert into register_entry(id, loan_id, date, description, debit, credit) values(?, ?, ?, ?, ?, ?)")) {
            ps.setBytes(1, UUIDToBytes.uuidToBytes(registerEntry.getId()));
            ps.setBytes(2, UUIDToBytes.uuidToBytes(registerEntry.getLoanId()));
            ps.setDate(3, java.sql.Date.valueOf(registerEntry.getDate()));
            ps.setString(4, registerEntry.getDescription());
            ps.setBigDecimal(5, registerEntry.getDebit());
            ps.setBigDecimal(6, registerEntry.getCredit());
            ps.executeUpdate();
        }
    }
}
