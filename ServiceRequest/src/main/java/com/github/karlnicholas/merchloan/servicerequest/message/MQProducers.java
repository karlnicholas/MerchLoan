package com.github.karlnicholas.merchloan.servicerequest.message;

import com.github.karlnicholas.merchloan.jms.MQConsumerUtils;
import com.github.karlnicholas.merchloan.jmsmessage.*;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.io.IOException;

@Service
@Slf4j
public class MQProducers {
    private final MQConsumerUtils mqConsumerUtils;
    private final Channel serviceRequestSenderChannel;

    @Autowired
    public MQProducers(Connection connection, MQConsumerUtils mqConsumerUtils) throws IOException {
        this.mqConsumerUtils = mqConsumerUtils;
        serviceRequestSenderChannel = connection.createChannel();
    }

    public void accountCreateAccount(CreateAccount createAccount) throws IOException {
        log.debug("accountCreateAccount: {}", createAccount);
        serviceRequestSenderChannel.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCreateAccountQueue(), null, SerializationUtils.serialize(createAccount));
    }

    public void accountFundLoan(FundLoan fundLoan) throws IOException {
        log.debug("accountFundLoan: {}", fundLoan);
        serviceRequestSenderChannel.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountFundingQueue(), null, SerializationUtils.serialize(fundLoan));
    }

    public void accountValidateCredit(CreditLoan creditLoan) throws IOException {
        log.debug("accountValidateCredit: {}", creditLoan);
        serviceRequestSenderChannel.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateCreditQueue(), null, SerializationUtils.serialize(creditLoan));
    }

    public void accountValidateDebit(DebitLoan debitLoan) throws IOException {
        log.debug("accountValidateDebit: {}", debitLoan);
        serviceRequestSenderChannel.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountValidateDebitQueue(), null, SerializationUtils.serialize(debitLoan));
    }

    public void statementStatement(StatementHeader statementHeader) throws IOException {
        log.debug("statementStatement: {}", statementHeader);
        serviceRequestSenderChannel.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getStatementStatementQueue(), null, SerializationUtils.serialize(statementHeader));
    }

    public void accountCloseLoan(CloseLoan closeLoan) throws IOException {
        log.debug("accountCloseLoan: {}", closeLoan);
        serviceRequestSenderChannel.basicPublish(mqConsumerUtils.getExchange(), mqConsumerUtils.getAccountCloseLoanQueue(), null, SerializationUtils.serialize(closeLoan));
    }

}
