package edu.auth.jetproud.exceptions;

public class ProudException extends Exception
{

    public ProudException() {
        super();
    }

    public ProudException(String message) {
        super(message);
    }

    public ProudException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProudException(Throwable cause) {
        super(cause);
    }

    public ProudException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
