package edu.auth.jetproud.proud.extension.proxy;

import edu.auth.jetproud.exceptions.ProudException;

import java.lang.reflect.Method;

public class ClassExtensionProxyException extends ProudException
{
    public ClassExtensionProxyException() {
    }

    public ClassExtensionProxyException(String message) {
        super(message);
    }

    public ClassExtensionProxyException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClassExtensionProxyException(Throwable cause) {
        super(cause);
    }

    public ClassExtensionProxyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static ClassExtensionProxyException noForwardingMethodFound(Class<?> targetClass, Method method) {
        String targetName = targetClass.getName();
        String methodSignature = method.toGenericString();

        return new ClassExtensionProxyException("Failed to forward method ("+methodSignature+") to instance of type '"+targetName+"' .");
    }
}
