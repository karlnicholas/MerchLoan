package com.github.karlnicholas.merchloan.jms;

import lombok.Builder;
import lombok.Data;

import java.util.Optional;
import java.util.UUID;

@Data
@Builder
public class ReplyWaiting {
    private Object reply;
    private long nanoTime;
    private UUID loanId;
    public Optional<Object> checkReply() {
        return Optional.ofNullable(reply);
    }
}
