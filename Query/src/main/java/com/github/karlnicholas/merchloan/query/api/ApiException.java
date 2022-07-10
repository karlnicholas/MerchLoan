package com.github.karlnicholas.merchloan.query.api;

public class ApiException extends RuntimeException{
    public ApiException(InterruptedException e) {
        super(e);
    }
}
