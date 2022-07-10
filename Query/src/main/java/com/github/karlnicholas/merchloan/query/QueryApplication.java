package com.github.karlnicholas.merchloan.query;

import com.github.karlnicholas.merchloan.jms.queue.QueueMessageService;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.api.core.client.ServerLocator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

@SpringBootApplication(scanBasePackages = {"com.github.karlnicholas.merchloan"})
public class QueryApplication {

    public static void main(String[] args) {
        SpringApplication.run(QueryApplication.class, args);
    }

    @Autowired
    private QueueMessageService queueMessageService;
    @Autowired
    private ServerLocator locator;

    @EventListener(ApplicationReadyEvent.class)
    public void applicationStarted() throws Exception {
        queueMessageService.initialize(locator, "query");
    }
}
