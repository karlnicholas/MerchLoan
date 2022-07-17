package com.github.karlnicholas.merchloan.servicerequest.dao;

import com.github.karlnicholas.merchloan.apimessage.message.ServiceRequestMessage;
import com.github.karlnicholas.merchloan.servicerequest.model.ServiceRequest;
import com.github.karlnicholas.merchloan.sqlutil.SqlUtils;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Slf4j
public class ServiceRequestDao {
//  id, request, request_type, local_date_time, status, status_message, retry_count

    public Optional<ServiceRequest> findById(Connection con, UUID id) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select id, request, request_type, local_date_time, status, status_message, retry_count from service_request where id = ?")) {
            ps.setBytes(1, SqlUtils.uuidToBytes(id));
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return Optional.of(ServiceRequest.builder()
                            .id(SqlUtils.toUUID(rs.getBytes(1)))
                            .request(rs.getString(2))
                            .requestType(rs.getString(3))
                            .localDateTime(rs.getTimestamp(4).toLocalDateTime())
                            .status(ServiceRequestMessage.STATUS.values()[rs.getInt(5)])
                            .statusMessage(rs.getString(6))
                            .retryCount(rs.getInt(7))
                            .build());
                else
                    return Optional.empty();
            }
        }
    }

    public List<ServiceRequest> findByStatus(Connection con, ServiceRequestMessage.STATUS status) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select id, request, request_type, local_date_time, status, status_message, retry_count from service_request where status = ?")) {
            ps.setInt(1, status.ordinal());
            try ( ResultSet rs = ps.executeQuery() ) {
                List<ServiceRequest> serviceRequests = new ArrayList<>();
                while (rs.next()) {
                    serviceRequests.add(ServiceRequest.builder()
                            .id(SqlUtils.toUUID(rs.getBytes(1)))
                            .request(rs.getString(2))
                            .requestType(rs.getString(3))
                            .localDateTime(rs.getTimestamp(4).toLocalDateTime())
                            .status(ServiceRequestMessage.STATUS.values()[rs.getInt(5)])
                            .statusMessage(rs.getString(6))
                            .retryCount(rs.getInt(7))
                            .build());
                }
                return serviceRequests;
            }
        }
    }

    public void update(Connection con, ServiceRequest serviceRequest) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("update service_request set status = ?, status_message = ?, retry_count = ? where id = ?")) {
            ps.setInt(1, serviceRequest.getStatus().ordinal());
            ps.setString(2, serviceRequest.getStatusMessage());
            ps.setInt(3, serviceRequest.getRetryCount());
            ps.setBytes(4, SqlUtils.uuidToBytes(serviceRequest.getId()));
            ps.executeUpdate();
        }
    }

    public void insert(Connection con, ServiceRequest serviceRequest) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("insert into service_request(id, request, request_type, local_date_time, status, status_message, retry_count) values(?, ?, ?, ?, ?, ?, ?)")) {
            ps.setBytes(1, SqlUtils.uuidToBytes(serviceRequest.getId()));
            ps.setString(2, serviceRequest.getRequest());
            ps.setString(3, serviceRequest.getRequestType());
            ps.setTimestamp(4, Timestamp.valueOf(serviceRequest.getLocalDateTime()));
            ps.setInt(5, serviceRequest.getStatus().ordinal());
            ps.setString(6, serviceRequest.getStatusMessage());
            ps.setInt(7, serviceRequest.getRetryCount());
            ps.executeUpdate();
        }
    }

    public Boolean existsStillProcessing(Connection con) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement("select count(*) as count from service_request where status in (" + ServiceRequestMessage.STATUS.PENDING.ordinal() + "," + ServiceRequestMessage.STATUS.ERROR.ordinal() + ")")) {
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next())
                    return rs.getInt(1) > 0;
                else
                    return Boolean.FALSE;
            }
        }
    }
}
