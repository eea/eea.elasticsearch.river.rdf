package org.elasticsearch.app.api.server.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(HttpStatus.CONFLICT)
public class CouldNotCloneIndex extends RuntimeException {
    public CouldNotCloneIndex() {
    }

    public CouldNotCloneIndex(String message) {
        super(message);
    }

    public CouldNotCloneIndex(String message, Throwable cause) {
        super(message, cause);
    }

    public CouldNotCloneIndex(Throwable cause) {
        super(cause);
    }

    public CouldNotCloneIndex(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
