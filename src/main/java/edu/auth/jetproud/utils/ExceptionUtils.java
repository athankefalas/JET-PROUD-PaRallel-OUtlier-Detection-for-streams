package edu.auth.jetproud.utils;

public final class ExceptionUtils {

    private ExceptionUtils(){}

    public static RuntimeException never() {
        return new RuntimeException("This exception should never be thrown. If you see this error please report a bug.");
    }

    public static <T extends Throwable> RuntimeException sneaky(T throwable) {
        return new RuntimeException(throwable);
    }

}
