package com.github.karlnicholas.merchloan.businessdate.businessdate.controller;

import com.github.karlnicholas.merchloan.businessdate.businessdate.model.BusinessDate;
import com.github.karlnicholas.merchloan.businessdate.businessdate.service.BusinessDateService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;

@RestController
@RequestMapping("/api")
public class BusinessDateController {
    private final BusinessDateService businessDateService;

    public BusinessDateController(BusinessDateService businessDateService) {
        this.businessDateService = businessDateService;
    }

    @PostMapping(value = "businessdate", consumes = MediaType.TEXT_PLAIN_VALUE)
    public void postBusinessDate(@RequestBody String businessDate) {
        BusinessDate priorBusinessDate = businessDateService.updateBusinessDate(LocalDate.parse(businessDate));
    }
}
