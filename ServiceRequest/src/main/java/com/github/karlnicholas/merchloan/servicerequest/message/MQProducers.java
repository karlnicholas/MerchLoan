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

        accountCreateAccountQueue = new ActiveMQQueue(mqConsumerUtils.getAccountCreateAccountQueue());
        accountFundingQueue = new ActiveMQQueue(mqConsumerUtils.getAccountFundingQueue());
        accountValidateCreditQueue = new ActiveMQQueue(mqConsumerUtils.getAccountValidateCreditQueue());
        accountValidateDebitQueue = new ActiveMQQueue(mqConsumerUtils.getAccountValidateDebitQueue());
        statementStatementQueue = new ActiveMQQueue(mqConsumerUtils.getStatementStatementQueue());
        accountCloseLoanQueue = new ActiveMQQueue(mqConsumerUtils.getAccountCloseLoanQueue());
    }

    public void accountCreateAccount(CreateAccount createAccount) throws JMSException {
        log.debug("accountCreateAccount: {}", createAccount);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(accountCreateAccountQueue, session.createObjectMessage(createAccount));
        }
    }

    public void accountFundLoan(FundLoan fundLoan) throws JMSException {
        log.debug("accountFundLoan: {}", fundLoan);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(accountFundingQueue, session.createObjectMessage(fundLoan));
        }
    }

    public void accountValidateCredit(CreditLoan creditLoan) throws JMSException {
        log.debug("accountValidateCredit: {}", creditLoan);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(accountValidateCreditQueue, session.createObjectMessage(creditLoan));

        }
    }

    public void accountValidateDebit(DebitLoan debitLoan) throws JMSException {
        log.debug("accountValidateDebit: {}", debitLoan);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(accountValidateDebitQueue, session.createObjectMessage(debitLoan));
        }
    }

    public void statementStatement(StatementHeader statementHeader) throws JMSException {
        log.debug("statementStatement: {}", statementHeader);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(statementStatementQueue, session.createObjectMessage(statementHeader));
        }
    }

    public void accountCloseLoan(CloseLoan closeLoan) throws JMSException {
        log.debug("accountCloseLoan: {}", closeLoan);
        try (Session session = connectionFactory.createConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            session.createProducer().send(accountCloseLoanQueue, session.createObjectMessage(closeLoan));
        }
    }

}
