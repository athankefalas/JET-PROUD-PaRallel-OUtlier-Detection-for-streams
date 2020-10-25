package edu.auth.jetproud.utils;

public final class ExceptionUtils {

    private ExceptionUtils(){}

    public static <T extends Throwable> RuntimeException sneaky(T throwable) {
        return new RuntimeException(throwable);
    }

}
