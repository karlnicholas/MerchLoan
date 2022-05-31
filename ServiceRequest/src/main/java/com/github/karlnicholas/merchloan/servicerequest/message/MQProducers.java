package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.activemq.artemis.jms.client.ActiveMQQueue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import jakarta.jms.*;

@Service
@Slf4j
public class MQProducers {
    private final ConnectionFactory connectionFactory;
    private final Queue accountCreateAccountQueue;
    private final Queue accountFundingQueue;
    private final Queue accountValidateCreditQueue;
    private final Queue accountValidateDebitQueue;
    private final Queue statementStatementQueue;
    private final Queue accountCloseLoanQueue;

    @Autowired
    public MQProducers(ConnectionFactory connectionFactory, MQConsumerUtils mqConsumerUtils) throws JMSException {
        this.connectionFactory = connectionFactory;

        accountCreateAccountQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountCreateAccountQueue());
        accountFundingQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountFundingQueue());
        accountValidateCreditQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountValidateCreditQueue());
        accountValidateDebitQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountValidateDebitQueue());
        statementStatementQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getStatementStatementQueue());
        accountCloseLoanQueue = ActiveMQQueue.createQueue(mqConsumerUtils.getAccountCloseLoanQueue());
    }

    public void accountCreateAccount(CreateAccount createAccount) throws JMSException {
        log.debug("accountCreateAccount: {}", createAccount);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountCreateAccountQueue, jmsContext.createObjectMessage(createAccount));
        }
    }

    public void accountFundLoan(FundLoan fundLoan) throws JMSException {
        log.debug("accountFundLoan: {}", fundLoan);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountFundingQueue, jmsContext.createObjectMessage(fundLoan));
        }
    }

    public void accountValidateCredit(CreditLoan creditLoan) throws JMSException {
        log.debug("accountValidateCredit: {}", creditLoan);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountValidateCreditQueue, jmsContext.createObjectMessage(creditLoan));

        }
    }

    public void accountValidateDebit(DebitLoan debitLoan) throws JMSException {
        log.debug("accountValidateDebit: {}", debitLoan);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountValidateDebitQueue, jmsContext.createObjectMessage(debitLoan));
        }
    }

    public void statementStatement(StatementHeader statementHeader) throws JMSException {
        log.debug("statementStatement: {}", statementHeader);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(statementStatementQueue, jmsContext.createObjectMessage(statementHeader));
        }
    }

    public void accountCloseLoan(CloseLoan closeLoan) throws JMSException {
        log.debug("accountCloseLoan: {}", closeLoan);
        try (JMSContext jmsContext = connectionFactory.createContext()) {
            jmsContext.createProducer().setDeliveryMode(DeliveryMode.NON_PERSISTENT).send(accountCloseLoanQueue, jmsContext.createObjectMessage(closeLoan));
        }
    }

}
