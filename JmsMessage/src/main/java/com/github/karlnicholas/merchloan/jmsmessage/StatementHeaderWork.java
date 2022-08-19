package com.github.karlnicholas.merchloan.jmsmessage;

import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class StatementHeaderWork implements Serializable {
    private StatementHeader statementHeader;
    private BillingCycleCharge billingCycleCharge;
    private Boolean lastStatementPresent;
    private BigDecimal lastStatementEndingBalance;
}
