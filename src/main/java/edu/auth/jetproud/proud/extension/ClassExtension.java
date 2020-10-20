package edu.auth.jetproud.proud.extension;

import java.lang.annotation.*;

@Target(value = { ElementType.METHOD })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface ClassExtension {
    Class<? extends ClassExtender> value();
}
