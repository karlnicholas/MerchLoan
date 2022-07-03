package com.github.karlnicholas.merchloan.businessdate.model;

import lombok.*;

import java.time.LocalDate;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class BusinessDate {
    private Long id;
    private LocalDate date;
}
