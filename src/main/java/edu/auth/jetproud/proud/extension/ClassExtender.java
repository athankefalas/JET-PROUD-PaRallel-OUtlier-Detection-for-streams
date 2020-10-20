package edu.auth.jetproud.proud.extension;

import java.util.UUID;

public interface ClassExtender<OriginalSelf> {

    UUID extenderId();
    String extenderName();

    void setTarget(OriginalSelf originalSelf);

}
