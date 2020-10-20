package edu.auth.jetproud.proud.partitioning.exceptions;

import edu.auth.jetproud.exceptions.ProudException;

import java.io.FileNotFoundException;

public class ProudPartitioningException extends ProudException
{
    public ProudPartitioningException() {
    }

    public ProudPartitioningException(String message) {
        super(message);
    }

    public ProudPartitioningException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProudPartitioningException(Throwable cause) {
        super(cause);
    }

    public ProudPartitioningException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public static ProudPartitioningException dataSampleError(Throwable cause) {
        return new ProudPartitioningException("Failed to read data set sample.", cause);
    }

    public static ProudPartitioningException emptyDataSampleError() {
        return new ProudPartitioningException("Failed to read data set sample. The provided file was empty.");
    }

    public static ProudPartitioningException dataSampleParseError(int line) {
        return new ProudPartitioningException("Failed to read data set sample. Could not parse data point at line "+line+".");
    }

    public static ProudPartitioningException internalPartitioningError(String message) {
        return new ProudPartitioningException("An internal error occurred while partitioning data. Details: "+message+".");
    }

}
