package com.github.karlnicholas.merchloan.servicerequest;

import com.github.karlnicholas.merchloan.servicerequest.api.ServiceRequestController;
import com.github.karlnicholas.merchloan.sqlutil.SqlInitialization;
import jakarta.ws.rs.ApplicationPath;
import jakarta.ws.rs.core.Application;
import org.apache.activemq.artemis.api.core.ActiveMQException;

import java.util.Set;

@ApplicationPath("/servicerequest")
public class ServiceRequestApplication extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return Set.of(ServiceRequestController.class);
    }

//    public void initialize() throws SQLException, IOException, ActiveMQException {
//        DataSource dataSource;
//        try(Connection con = dataSource.getConnection()) {
//            SqlInitialization.initialize(con, ServiceRequestApplication.class.getResourceAsStream("/sql/schema.sql"));
//        }
//    }
}
