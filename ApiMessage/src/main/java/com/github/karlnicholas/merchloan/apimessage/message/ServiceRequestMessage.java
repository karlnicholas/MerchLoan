package com.github.karlnicholas.merchloan.apimessage.message;

import java.io.Serializable;

public interface ServiceRequestMessage extends Serializable {
    enum STATUS {PENDING, WAITING, SUCCESS, ERROR, FAILURE}
}
