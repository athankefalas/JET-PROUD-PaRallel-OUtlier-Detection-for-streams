package edu.auth.jetproud.proud.extension.proxy;

import edu.auth.jetproud.proud.extension.ClassExtender;
import edu.auth.jetproud.proud.extension.ClassExtension;
import edu.auth.jetproud.utils.Lists;
import edu.auth.jetproud.utils.Tuple;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

public final class ClassExtensionInvocationHandler<Target> implements InvocationHandler
{

    private final Target forwardingTarget;
    private final Map<UUID, ClassExtender<Target>> extenders;

    private final Map<String, List<Method>> forwardingTargetMethodCache;

    private final Map<String, UUID> extenderUUIDTable;
    private final Map<UUID, Map<String, List<Method>>> extenderMethodCache;

    // Prevent Default Init
    private ClassExtensionInvocationHandler() {
        throw new RuntimeException("JetPipelineInvocationHandler cannot be created with the default constructor.");
    }

    public ClassExtensionInvocationHandler(Target forwardingTarget) {
        this.forwardingTarget = forwardingTarget;
        this.extenders = new HashMap<>();

        this.forwardingTargetMethodCache = new HashMap<>();

        this.extenderUUIDTable = new HashMap<>();
        this.extenderMethodCache = new HashMap<>();

        // Create Reflection Cache to avoid costly method lookup
        buildTargetReflectionCache();
    }

    public <Extender extends ClassExtender<Target>> void addExtender(Extender extender) {
        if (extender == null)
            throw new IllegalArgumentException("JetPipelineExtender must not be null.");

        UUID extenderId = extender.extenderId();
        Class<ClassExtender<Target>> extenderClass = (Class<ClassExtender<Target>>) extender.getClass();

        extenders.put(extenderId, extender);
        extenderUUIDTable.put(extenderClass.getName(), extenderId);

        buildExtenderReflectionCache(extenderClass, extenderId);
    }

    // Caching

    private String methodSignatureKeyOf(Method method) {
        if (method == null)
            return "null";
        StringBuilder signatureBuilder = new StringBuilder();
        boolean isAccessible = Modifier.isPublic(method.getModifiers());

        if (!isAccessible) {
            signatureBuilder.append("_");
        }

        Class<?> ret = method.getReturnType();
        String returnTypeName = ret.getName();

        boolean isVoid = returnTypeName.equals("void")
                || returnTypeName.equals("java.lang.Void")
                || returnTypeName.equals("");

        if (isVoid) {
            signatureBuilder.append(" 0 ");
        } else {
            signatureBuilder.append(" 1 ");
        }

        signatureBuilder.append(method.getName())
                .append("(")
                .append(method.getParameters().length)
                .append(")");

        return signatureBuilder.toString();
    }

    private void buildTargetReflectionCache() {
        forwardingTargetMethodCache.clear();

        for (Method method:forwardingTarget.getClass().getMethods()) {
            String key = methodSignatureKeyOf(method);

            if (!forwardingTargetMethodCache.containsKey(key)) {
                forwardingTargetMethodCache.put(key, Lists.make());
            }

            List<Method> methods = forwardingTargetMethodCache.get(key);
            methods.add(method);

            forwardingTargetMethodCache.put(key, methods);
        }
    }

    private <Extender extends ClassExtender<Target>> void buildExtenderReflectionCache(Class<Extender> extenderClass, UUID extenderId) {
        Map<String, List<Method>> extenderCache = new HashMap<>();

        for (Method method:extenderClass.getMethods()) {
            String key = methodSignatureKeyOf(method);

            if (!extenderCache.containsKey(key)) {
                extenderCache.put(key, Lists.make());
            }

            List<Method> methods = extenderCache.get(key);
            methods.add(method);

            extenderCache.put(key, methods);
        }

        extenderMethodCache.put(extenderId, extenderCache);
    }

    private ClassExtender<Target> extenderInstance(Class<?> extenderClass) {
        String extenderKey = extenderClass.getName();
        UUID extenderId = extenderUUIDTable.get(extenderKey);
        return extenders.get(extenderId);
    }

    private Method forwardingTargetMethod(Method method) {
        String key = methodSignatureKeyOf(method);
        List<Method> methods = forwardingTargetMethodCache.get(key);

        if (methods.isEmpty())
            return null;

        if (methods.size() == 1)
            return methods.get(0);

        return firstMatch(method, methods);
    }

    private Method extenderMethod(Class<?> extenderClass, Method method) {
        String extenderKey = extenderClass.getName();
        UUID extenderId = extenderUUIDTable.get(extenderKey);
        String methodKey = methodSignatureKeyOf(method);

        Map<String, List<Method>> extenderCache = extenderMethodCache.get(extenderId);

        if (extenderCache == null)
            return null;

        List<Method> methods = extenderCache.get(methodKey);

        if (methods.isEmpty())
            return null;

        if (methods.size() == 1)
            return methods.get(0);

        return firstMatch(method, methods);
    }

    private Method firstMatch(Method method, List<Method> candidates) {
        List<Method> assignableCandidates = candidates.stream()
                .filter((it) -> canForwardCall(method, it))
                .collect(Collectors.toList());

        if (assignableCandidates.isEmpty())
            return null;

        if (assignableCandidates.size() == 1)
            return assignableCandidates.get(0);

        List<Tuple<Integer,Method>> directMatches = assignableCandidates.stream()
                .map((it) -> new Tuple<>(forwardPriority(method, it), it))
                .filter((it) -> it.first >= 0)
                .sorted(Comparator.comparingInt(Tuple::getFirst))
                .collect(Collectors.toList());

        if (directMatches.isEmpty())
            return null;

        if (directMatches.size() == 1)
            return directMatches.get(0).second;

        return directMatches.get(directMatches.size() - 1).second;
    }

    // Forwarding invocation implementation

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        // Get JetPipelineExtension if present or null
        ClassExtension classExtensionAnnotation = method.getAnnotation(ClassExtension.class);

        // If no JetPipelineExtension annotation is not present
        // then forward to actual Jet target
        if (classExtensionAnnotation == null) {
            Method targetMethod = forwardingTargetMethod(method);
            if (targetMethod == null) {
                throw ClassExtensionProxyException.noForwardingMethodFound(forwardingTarget.getClass(), method);
            }

            return targetMethod.invoke(forwardingTarget, args);
        } else {
            // else find the extender marked in the annotation to forward the invocation
            Class <?> extenderClass = classExtensionAnnotation.value();

            ClassExtender<Target> extender = extenderInstance(extenderClass);
            Method targetMethod = extenderMethod(extenderClass, method);

            if (targetMethod == null) {
                throw ClassExtensionProxyException.noForwardingMethodFound(extenderClass, method);
            }

            extender.setTarget(forwardingTarget);
            return targetMethod.invoke(extender, args);
        }
    }

    private boolean canForwardCall(Method origin, Method target) {
        Class<?> originReturnType = origin.getReturnType();
        Class<?> targetReturnType = target.getReturnType();

        boolean returnTypeMatches = originReturnType.isAssignableFrom(targetReturnType);

        if (!returnTypeMatches)
            return false;

        for (int i=0; i < origin.getParameterTypes().length; i++) {
            Class<?> originParamType = origin.getReturnType();
            Class<?> targetParamType = target.getReturnType();

            boolean paramTypeMatches = targetParamType.isAssignableFrom(originParamType);

            if (!paramTypeMatches)
                return false;
        }

        return true;
    }

    private int forwardPriority(Method origin, Method target) {
        final int INCOMPATIBLE = -1;
        int priority = 0;

        Class<?> originReturnType = origin.getReturnType();
        Class<?> targetReturnType = target.getReturnType();

        String originReturnTypeName = originReturnType.getCanonicalName();
        String targetReturnTypeName = targetReturnType.getCanonicalName();

        boolean returnTypesAssignable = originReturnType.isAssignableFrom(targetReturnType);

        if (returnTypesAssignable)
            priority++;
        else
            return INCOMPATIBLE;

        boolean returnTypeMatches = originReturnTypeName.equals(targetReturnTypeName);

        if (returnTypeMatches)
            priority++;

        for (int i=0; i < origin.getParameterTypes().length; i++) {
            Class<?> originParamType = origin.getReturnType();
            Class<?> targetParamType = target.getReturnType();

            String originParamTypeName = originParamType.getCanonicalName();
            String targetParamTypeName = targetParamType.getCanonicalName();

            boolean paramTypeAssignable = targetParamType.isAssignableFrom(originParamType);

            if (paramTypeAssignable)
                priority++;
            else
                return INCOMPATIBLE;

            boolean paramTypeMatches = originParamTypeName.equals(targetParamTypeName);

            if (paramTypeMatches)
                priority++;
        }

        return priority;
    }

}
