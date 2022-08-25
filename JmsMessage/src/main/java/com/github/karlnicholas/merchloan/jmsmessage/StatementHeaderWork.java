package com.github.karlnicholas.merchloan.jmsmessage;

import lombok.*;

import java.io.Serializable;
import java.math.BigDecimal;

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
