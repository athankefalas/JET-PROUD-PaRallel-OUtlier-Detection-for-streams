package edu.auth.jetproud.exceptions;

public class ParserDeserializationException extends ProudException
{

    public ParserDeserializationException() {
        this("Parser failed to parse value from string.");
    }

    public ParserDeserializationException(String serializedString, String objectName) {
        this("Parser failed to parse "+objectName+" value from  string '"+serializedString+"'.");
    }

    public ParserDeserializationException(String message) {
        super(message);
    }
}
