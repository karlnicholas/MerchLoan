package com.github.karlnicholas.merchloan.accounts.model;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Loan {
    public enum LOAN_STATE {OPEN, CLOSED}
//    @Id
//    @Column(columnDefinition = "BINARY(16)")
    private UUID id;
//    @ManyToOne
    private UUID accountId;
    private LocalDate startDate;
//    @Convert(converter = StatementDatesConverter.class)
    private List<LocalDate> statementDates;
    private BigDecimal funding;
    private Integer months;
    private BigDecimal interestRate;
    private BigDecimal monthlyPayments;
    private LOAN_STATE loanState;
}
