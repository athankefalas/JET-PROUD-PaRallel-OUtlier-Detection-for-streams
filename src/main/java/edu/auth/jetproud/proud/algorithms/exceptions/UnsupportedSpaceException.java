package edu.auth.jetproud.proud.algorithms.exceptions;

import edu.auth.jetproud.application.parameters.data.ProudAlgorithmOption;
import edu.auth.jetproud.application.parameters.data.ProudSpaceOption;
import edu.auth.jetproud.exceptions.ProudException;

public class UnsupportedSpaceException extends ProudException
{
    public UnsupportedSpaceException(String message) {
        super(message);
    }

    public UnsupportedSpaceException(ProudSpaceOption spaceOption, ProudAlgorithmOption algorithmOption) {
        this("Algorithm "+algorithmOption.name()+" doesn't support space option '"+spaceOption.name()+"'.");
    }
}
