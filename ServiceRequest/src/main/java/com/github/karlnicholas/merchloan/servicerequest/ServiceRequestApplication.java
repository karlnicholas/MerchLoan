package com.github.karlnicholas.merchloan.servicerequest;

import com.github.karlnicholas.merchloan.sqlutil.SqlInitialization;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

@SpringBootApplication(scanBasePackages = {"com.github.karlnicholas.merchloan"})
@EnableScheduling
@EnableAsync
public class ServiceRequestApplication {

    public static void main(String[] args) {
        SpringApplication.run(ServiceRequestApplication.class, args);
    }

    @Autowired
    private DataSource dataSource;
    @Autowired
    private ClientSession clientSession;

    @EventListener(ApplicationReadyEvent.class)
    public void initialize() throws SQLException, IOException, ActiveMQException {
        try(Connection con = dataSource.getConnection()) {
            SqlInitialization.initialize(con, ServiceRequestApplication.class.getResourceAsStream("/sql/schema.sql"));
        }
        clientSession.start();
    }
}
