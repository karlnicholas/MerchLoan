package com.github.karlnicholas.merchloan.accountsdb;

public class AccountsDbApplication {
    public static void main(String[] args) throws Exception {
        new AccountsDbApplication().run();
    }

    public void run() throws Exception {
        AccountsDb.startServer();
    }

}
