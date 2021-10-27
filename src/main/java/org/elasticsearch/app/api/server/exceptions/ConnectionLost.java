package org.elasticsearch.app.api.server.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
public class ConnectionLost extends RuntimeException {
    public ConnectionLost() {
    }

    public ConnectionLost(String message) {
        super(message);
    }

    public ConnectionLost(String message, Throwable cause) {
        super(message, cause);
    }

    public ConnectionLost(Throwable cause) {
        super(cause);
    }

    public ConnectionLost(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
