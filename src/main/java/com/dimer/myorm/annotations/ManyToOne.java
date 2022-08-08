package com.dimer.myorm.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface ManyToOne {
    String column();

    FetchType fetchType() default FetchType.EAGER;
}
