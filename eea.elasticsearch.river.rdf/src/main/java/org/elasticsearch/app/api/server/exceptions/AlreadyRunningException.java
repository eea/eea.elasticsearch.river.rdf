package org.elasticsearch.app.ApiSpringServer.Exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.PROCESSING)
public class AlreadyRunningException extends RuntimeException {
    public AlreadyRunningException() {
    }

    public AlreadyRunningException(String message) {
        super(message);
    }

    public AlreadyRunningException(String message, Throwable cause) {
        super(message, cause);
    }

    public AlreadyRunningException(Throwable cause) {
        super(cause);
    }

    public AlreadyRunningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
