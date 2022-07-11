package com.github.karlnicholas.merchloan.servicerequest.component;

import java.util.UUID;

@FunctionalInterface
public interface ExceptionFunction<T, R> {
    R route(T t, Boolean retry, UUID existingId) throws ServiceRequestException;
}
