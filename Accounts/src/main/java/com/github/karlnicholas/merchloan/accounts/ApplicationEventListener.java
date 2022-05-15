package com.github.karlnicholas.merchloan.accounts;

import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

@Component
public class ApplicationEventListener implements ApplicationListener<ApplicationEvent> {
    @Override
    public void onApplicationEvent(ApplicationEvent applicationEvent) {
        System.out.println(applicationEvent);
//        if ( applicationEvent instanceof ContextClosedEvent) {
//            System.out.println(applicationEvent);
//        }
    }
}