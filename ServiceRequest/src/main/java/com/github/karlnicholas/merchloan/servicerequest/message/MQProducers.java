package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.client.ClientMessage;
import org.apache.activemq.artemis.api.core.client.ClientProducer;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

@Service
@Slf4j
public class MQProducers {
    private final ClientSession clientSession;
    private final ClientProducer accountCreateAccountProducer;
    private final ClientProducer accountFundingProducer;
    private final ClientProducer accountValidateCreditProducer;
    private final ClientProducer accountValidateDebitProducer;
    private final ClientProducer statementStatementProducer;
    private final ClientProducer accountCloseLoanProducer;

    @Autowired
    public MQProducers(ClientSession clientSession, MQConsumerUtils mqConsumerUtils) throws ActiveMQException {
        this.clientSession = clientSession;
        accountCreateAccountProducer = clientSession.createProducer(mqConsumerUtils.getAccountCreateAccountQueue());
        accountFundingProducer = clientSession.createProducer(mqConsumerUtils.getAccountFundingQueue());
        accountValidateCreditProducer = clientSession.createProducer(mqConsumerUtils.getAccountValidateCreditQueue());
        accountValidateDebitProducer = clientSession.createProducer(mqConsumerUtils.getAccountValidateDebitQueue());
        statementStatementProducer = clientSession.createProducer(mqConsumerUtils.getStatementStatementQueue());
        accountCloseLoanProducer = clientSession.createProducer(mqConsumerUtils.getAccountCloseLoanQueue());
    }

    public void accountCreateAccount(CreateAccount createAccount) throws ActiveMQException {
        log.debug("accountCreateAccount: {}", createAccount);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(createAccount));
        accountCreateAccountProducer.send(message);
    }

    public void accountFundLoan(FundLoan fundLoan) throws ActiveMQException {
        log.debug("accountFundLoan: {}", fundLoan);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(fundLoan));
        accountFundingProducer.send(message);
    }

    public void accountValidateCredit(CreditLoan creditLoan) throws ActiveMQException {
        log.debug("accountValidateCredit: {}", creditLoan);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(creditLoan));
        accountValidateCreditProducer.send(message);
    }

    public void accountValidateDebit(DebitLoan debitLoan) throws ActiveMQException {
        log.debug("accountValidateDebit: {}", debitLoan);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(debitLoan));
        accountValidateDebitProducer.send(message);
    }

    public void statementStatement(StatementHeader statementHeader) throws ActiveMQException {
        log.debug("statementStatement: {}", statementHeader);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(statementHeader));
        statementStatementProducer.send(message);
    }

    public void accountCloseLoan(CloseLoan closeLoan) throws ActiveMQException {
        log.debug("accountCloseLoan: {}", closeLoan);
        ClientMessage message = clientSession.createMessage(false);
        message.getBodyBuffer().writeBytes(SerializationUtils.serialize(closeLoan));
        accountCloseLoanProducer.send(message);
    }

}
