package com.github.karlnicholas.merchloan.statementdb;

public class StatementDbApplication {
    public static void main(String[] args) throws Exception {
        new StatementDbApplication().run();
    }

    public void run() throws Exception {
        StatementDb.startServer();
    }

}
