package com.github.karlnicholas.merchloan.servicerequest.service;

import com.github.karlnicholas.merchloan.servicerequest.dao.ServiceRequestDao;
import com.github.karlnicholas.merchloan.servicerequest.model.ServiceRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

@Service
@Slf4j
public class QueryService {
    private final ServiceRequestDao serviceRequestDao;
    private final DataSource dataSource;

    public QueryService(ServiceRequestDao serviceRequestDao, DataSource dataSource) {
        this.serviceRequestDao = serviceRequestDao;
        this.dataSource = dataSource;
    }

    public Optional<ServiceRequest> getServiceRequest(UUID id) throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            return serviceRequestDao.findById(con, id);
        }
    }

    public Boolean checkRequest() throws SQLException {
        try (Connection con = dataSource.getConnection()) {
            return serviceRequestDao.existsStillProcessing(con);
        }
    }
}
