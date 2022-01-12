package com.github.karlnicholas.merchloan.statement.model;

import lombok.*;

import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.UUID;

@Entity
@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Statement {
    @EmbeddedId
    private StatementPK id;
    private BigDecimal startingBalance;
    private BigDecimal endingBalance;
    @Lob
    private String statement;
}
