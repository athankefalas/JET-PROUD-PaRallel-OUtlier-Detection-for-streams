package edu.auth.jetproud.application.parameters;

import java.lang.annotation.*;

@Target(value = { ElementType.FIELD })
@Retention(value = RetentionPolicy.RUNTIME)
public @interface ProudParameterValue {
    String Switch();
}
