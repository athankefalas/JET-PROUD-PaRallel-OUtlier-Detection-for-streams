package edu.auth.jetproud.application.parameters.errors;

import edu.auth.jetproud.application.parameters.ProudParameter;
import edu.auth.jetproud.exceptions.ProudException;

public class ProudArgumentException extends ProudException {

    public ProudArgumentException() {
        super();
    }

    public ProudArgumentException(String message) {
        super(message);
    }

    public static ProudArgumentException invalidArgumentSequence() {
        return new ProudArgumentException("The provided argument sequence is empty, missing a switch or value, or has an unescaped string.");
    }

    public static ProudArgumentException unknownSwitch(String _switch) {
        return new ProudArgumentException("Unrecognized switch '"+_switch+"'.");
    }

    public static ProudArgumentException unreadableValue(String value, String _switch) {
        return new ProudArgumentException("Unable to parse value '"+value+"' given for switch '"+_switch+"'.");
    }

    public static ProudArgumentException requiredSwitchMissing(String _switch) {
        return new ProudArgumentException("Switch '"+_switch+"' is missing from the given arguments.");
    }

    public static ProudArgumentException missing(String value, String container) {
        return new ProudArgumentException("Failed to find required value '"+value+"' in "+container+".");
    }

    public static ProudArgumentException invalid(String message) {
        return new ProudArgumentException("Illegal arguments: "+message+".");
    }

    public static ProudArgumentException internalParseError(String message) {
        return new ProudArgumentException("An internal error occurred while reading application arguments. Info: "+message+".");
    }
}
