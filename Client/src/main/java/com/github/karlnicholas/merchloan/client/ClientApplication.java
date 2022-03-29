package com.github.karlnicholas.merchloan.client;

import com.github.karlnicholas.merchloan.client.component.*;
import com.github.karlnicholas.merchloan.client.process.BusinessDateMonitor;
import com.github.karlnicholas.merchloan.client.process.LoanCycleThread;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.EventListener;

import java.time.LocalDate;
import java.util.concurrent.ThreadLocalRandom;

@SpringBootApplication
@Slf4j
public class ClientApplication {

    public static void main(String[] args) {
        SpringApplication.run(ClientApplication.class);
    }

    @Autowired
    private AccountComponent accountComponent;
    @Autowired
    private LoanComponent loanComponent;
    @Autowired
    private CreditComponent creditComponent;
    @Autowired
    private CloseComponent closeComponent;
    @Autowired
    private LoanStateComponent loanStateComponent;
    @Autowired
    private RequestStatusComponent requestStatusComponent;
    @Autowired
    private ApplicationEventPublisher applicationEventPublisher;
    @Autowired
    private BusinessDateComponent businessDateComponent;
    private BusinessDateMonitor businessDateMonitor;
//    private List<LoanCycleThread> threads;

    @EventListener(ApplicationReadyEvent.class)
    public void loadData(ApplicationReadyEvent event) {
        businessDateMonitor = new BusinessDateMonitor();
        createLoanListeners();
        runBusinessDateThread();
    }

    private void createLoanListeners() {
//        threads = new ArrayList<>();
        for ( int i =0; i < 50; ++i ) {
            int plusDays = ThreadLocalRandom.current().nextInt(30);
//            int plusDays = 0;
//            threads.add(new LoanCycleThread(creditComponent, accountComponent, loanComponent, loanStateComponent, requestStatusComponent, businessDateMonitor, LocalDate.now().plusDays(plusDays), "Client " + i));
            new LoanCycleThread(creditComponent, accountComponent, loanComponent, loanStateComponent, requestStatusComponent, businessDateMonitor, LocalDate.now().plusDays(plusDays), "Client " + i).start();
        }
//        threads.forEach(Thread::start);
    }

    private void runBusinessDateThread() {
        // do something
        new Thread(()->{
            try {
                LocalDate currentDate = LocalDate.now();
                LocalDate endDate = currentDate.plusYears(1).plusMonths(2);
                Thread.sleep(5000);
                int bdRetry;
                while ( currentDate.isBefore(endDate)) {
                    bdRetry = 0;
                    if ( !businessDateComponent.updateBusinessDate(currentDate) ) {
                        if ( ++bdRetry > 10 ) {
                            log.error("Business date failed to update");
                            return;
                        }
                        log.info("Business date not ready or did not update");
                        continue;
                    }
                    businessDateMonitor.newDate(currentDate);
                    if ( currentDate.getDayOfMonth() == 1 ) {
                        log.info("{}", currentDate);
                    }
                    currentDate = currentDate.plusDays(1);
                    Thread.sleep(500);
                }
                businessDateMonitor.newDate(null);
                log.info("DATES FINISHED AT {}", currentDate);
//                threads.forEach(LoanCycleThread::showStatement);
            } catch (InterruptedException e) {
                log.error("Simulation thread interrupted", e);
                Thread.currentThread().interrupt();
            }
        }).start();
    }

}
