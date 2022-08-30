package com.github.karlnicholas.merchloan.jms;

import lombok.Builder;
import lombok.Data;

import java.util.Optional;

@Data
@Builder
public class ReplyWaiting {
    private Object reply;
    private long nanoTime;
    public Optional<Object> checkReply() {
        return Optional.ofNullable(reply);
    }
}
