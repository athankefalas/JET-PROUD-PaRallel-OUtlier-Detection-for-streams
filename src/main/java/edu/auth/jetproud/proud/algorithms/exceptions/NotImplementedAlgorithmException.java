package edu.auth.jetproud.proud.algorithms.exceptions;

import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.exceptions.ProudException;

public class NotImplementedAlgorithmException extends ProudException {

    public NotImplementedAlgorithmException(String message) {
        super(message);
    }

    public NotImplementedAlgorithmException(ProudAlgorithmOption algorithmOption) {
        this("Algorithm '"+algorithmOption.name()+"' is not yet implemented.");
    }
}
