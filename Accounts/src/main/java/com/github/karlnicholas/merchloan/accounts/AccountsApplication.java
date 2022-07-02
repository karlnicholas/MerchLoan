package com.github.karlnicholas.merchloan.accounts;

import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;

@SpringBootApplication(scanBasePackages = {"com.github.karlnicholas.merchloan"})
@Slf4j
public class AccountsApplication {
    public static void main(String[] args) throws InterruptedException {
        ApplicationContext ctx = SpringApplication.run(AccountsApplication.class, args);
        final CountDownLatch closeLatch = ctx.getBean(CountDownLatch.class);
        Runtime.getRuntime().addShutdownHook(new Thread(closeLatch::countDown));
        closeLatch.await();
    }
    @Bean
    public CountDownLatch closeLatch() {
        return new CountDownLatch(1);
    }
    @Autowired
    private ClientSession clientSession;
    @EventListener(ApplicationReadyEvent.class)
    public void initialize() throws SQLException, IOException, ActiveMQException {
        clientSession.start();
    }
}
