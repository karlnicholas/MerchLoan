package com.github.karlnicholas.merchloan.client;

import com.github.karlnicholas.merchloan.client.component.*;
import com.github.karlnicholas.merchloan.client.config.HttpConnectionPoolConfig;
import com.github.karlnicholas.merchloan.client.process.LoanCycle;
import com.github.karlnicholas.merchloan.client.rest.LoanProcessQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
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
    private LoanStateComponent loanStateComponent;
    @Autowired
    private RequestStatusComponent requestStatusComponent;
    @Autowired
    private BusinessDateComponent businessDateComponent;
    @Autowired
    private LoanProcessQueue loanProcessQueue;
    @Autowired
    private HttpConnectionPoolConfig httpConnectionPoolConfig;
    private List<LoanCycle> loans;

    @EventListener(ApplicationReadyEvent.class)
    public void loadData(ApplicationReadyEvent event) throws InterruptedException {
        createLoanListeners();
        runBusinessDateThread();
    }

    private void createLoanListeners() {
        loans = new ArrayList<>();
        for ( int i =0; i < 100; ++i ) {
            int plusDays = ThreadLocalRandom.current().nextInt(30);
//             int plusDays = 0;
            loans.add(new LoanCycle(creditComponent, accountComponent, loanComponent, loanStateComponent, requestStatusComponent, LocalDate.now().plusDays(plusDays), "Client " + i));
        }
    }

    private void runBusinessDateThread() throws InterruptedException {
        // do something
        Thread thread = new Thread(()->{
            try {
                LocalDate currentDate = LocalDate.now();
                LocalDate endDate = currentDate.plusYears(1).plusMonths(2);
                Thread.sleep(5000);
                int bdRetry = 0;
                while ( currentDate.isBefore(endDate)) {
                    if ( loanProcessQueue.checkWorking() ) {
                        log.info("RestQueue still working {}", currentDate);
                        Thread.sleep(250);
                        continue;
                    }
                    if ( !businessDateComponent.updateBusinessDate(currentDate) ) {
                        if ( bdRetry > 100 ) {
                            log.error("Business date failed to update {}", currentDate);
                            return;
                        }
                        if ( bdRetry % 10 == 0 )
                            log.info("Business date not ready {} {}", currentDate, bdRetry);
                        bdRetry++;
                        Thread.sleep(250);
                        continue;
                    }
                    bdRetry = 0;
                    for( LoanCycle loan: loans) {
                        loan.cycle(loanProcessQueue, currentDate);
                    }
                    for( LoanCycle loan: loans) {
                        loan.updateState(currentDate);
                    }
                    if ( currentDate.getDayOfMonth() == 1 ) {
                        log.info("{}", currentDate);
                        PoolingHttpClientConnectionManager pcm = httpConnectionPoolConfig.getPoolingHttpClientConnectionManager();
                        pcm.getRoutes()
                                .forEach(r->log.info("route stats {} {}", r, pcm.getStats(r)));
                    }
                    currentDate = currentDate.plusDays(1);
                    Thread.sleep(250);
                }
                log.info("DATES FINISHED AT {}", currentDate);
                int[] first = new int[1];
                loans.forEach(loan->{
                    if ( !loan.checkClosed() )
                        loan.showStatement("Not Closed");
                    if ( first[0] == 0 ) {
                        loan.showStatement("Show Statement");
                        first[0] = 1;
                    }
                });
                PoolingHttpClientConnectionManager pcm = httpConnectionPoolConfig.getPoolingHttpClientConnectionManager();
                pcm.getRoutes()
                        .forEach(r->log.info("route stats {} {}", r, pcm.getStats(r)));

            } catch (InterruptedException e) {
                log.error("Simulation thread interrupted", e);
                Thread.currentThread().interrupt();
            }
        });
        thread.start();
        thread.join();
        httpConnectionPoolConfig.getPoolingHttpClientConnectionManager().close();
        loanProcessQueue.getExecutorService().shutdown();
    }

}
