package com.github.karlnicholas.merchloan.accounts.dao;

import com.github.karlnicholas.merchloan.accounts.model.Account;
import com.github.karlnicholas.merchloan.sqlutil.UUIDToBytes;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
public class AccountDao {
    public void createAccount(Connection con, Account account) throws SQLException {
        if (ThreadLocalRandom.current().nextDouble() > 0.999 )
            throw new SQLException("JUNK");
        try (PreparedStatement ps = con.prepareStatement("insert into account(id, create_date, customer) values(?, ?, ?)")) {
            ps.setBytes(1, UUIDToBytes.uuidToBytes(account.getId()));
            ps.setDate(2, java.sql.Date.valueOf(account.getCreateDate()));
            ps.setString(3, account.getCustomer());
            ps.executeUpdate();
        }
    }

    public Optional<Account> findById(Connection con, UUID accountId) throws SQLException {
        if (ThreadLocalRandom.current().nextDouble() > 0.999 )
            throw new SQLException("JUNK");
        try (PreparedStatement ps = con.prepareStatement("select id, create_date, customer from account where id = ?")) {
            ps.setBytes(1, UUIDToBytes.uuidToBytes(accountId));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return Optional.of(Account.builder()
                            .id(UUIDToBytes.toUUID(rs.getBytes(1)))
                            .createDate(rs.getDate(2).toLocalDate())
                            .customer(rs.getString(3))
                            .build());
                else
                    return Optional.empty();
            }
        }
    }
}
