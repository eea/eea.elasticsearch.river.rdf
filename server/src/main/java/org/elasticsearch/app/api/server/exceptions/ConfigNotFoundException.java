package org.elasticsearch.app.api.server.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.NOT_FOUND)
public class ConfigNotFoundException extends RuntimeException {
    public ConfigNotFoundException() {
    }

    public ConfigNotFoundException(String message) {
        super(message);
    }

    public ConfigNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConfigNotFoundException(Throwable cause) {
        super(cause);
    }

    public ConfigNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
