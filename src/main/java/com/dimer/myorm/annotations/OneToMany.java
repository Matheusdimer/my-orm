package com.dimer.myorm.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface OneToMany {
    String column();

    FetchType fetchType() default FetchType.LAZY;
}
