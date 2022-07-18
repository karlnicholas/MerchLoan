package com.github.karlnicholas.merchloan.query;

import com.github.karlnicholas.merchloan.query.api.QueryController;
import jakarta.ws.rs.ApplicationPath;
import jakarta.ws.rs.core.Application;

import java.util.Set;

@ApplicationPath("query")
public class QueryApplication extends Application {
    @Override
    public Set<Class<?>> getClasses() {
        return Set.of(QueryController.class);
    }

//    public void initialize() throws SQLException, IOException, ActiveMQException {
//        DataSource dataSource;
//        try(Connection con = dataSource.getConnection()) {
//            SqlInitialization.initialize(con, ServiceRequestApplication.class.getResourceAsStream("/sql/schema.sql"));
//        }
//    }
}
