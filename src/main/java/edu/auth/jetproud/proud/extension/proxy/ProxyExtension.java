package edu.auth.jetproud.proud.extension.proxy;

import edu.auth.jetproud.proud.extension.ClassExtender;

import java.lang.reflect.Proxy;

public final class ProxyExtension {

    private ProxyExtension(){}

    @SafeVarargs
    public static <T, P> P of(T target,
                              Class<P> extensionInterfaceClass,
                              ClassExtender<T> extender, ClassExtender<T>...others) {
        ClassExtensionInvocationHandler<T> invocationHandler = new ClassExtensionInvocationHandler<>(target);
        invocationHandler.addExtender(extender);

        if (others != null) {
            for (ClassExtender<T> currentExtender:others) {
                invocationHandler.addExtender(currentExtender);
            }
        }

        try {
            P extendedInstance = (P) Proxy.newProxyInstance(extensionInterfaceClass.getClassLoader(),
                    new Class[]{ extensionInterfaceClass },
                    invocationHandler
            );

            return extendedInstance;
        } catch (Exception e) {
            return null;
        }
    }

}
