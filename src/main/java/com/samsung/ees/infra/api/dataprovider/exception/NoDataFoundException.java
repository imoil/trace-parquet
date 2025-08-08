package com.samsung.ees.infra.api.dataprovider.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

/**
 * Custom exception for cases where no data is found for the given criteria.
 */
@ResponseStatus(HttpStatus.NOT_FOUND)
public class NoDataFoundException extends RuntimeException {
    public NoDataFoundException(String message) {
        super(message);
    }
}
