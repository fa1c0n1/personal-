package com.apple.aml.stargate.common.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD, ElementType.TYPE})
public @interface DesignerSpec {

    boolean required() default false;

    Type type() default Type.SingleLineText;

    String label() default "";

    String toolTip() default "";

    String documentation() default "";

    String[] choices() default {};

    String defaultValue() default "";

    long min() default 0;

    long max() default Long.MAX_VALUE;

    double minDouble() default 0;

    double maxDouble() default -999999.0;

    enum Type {
        SingleChoice,
        MultiChoice,
        YesNo,
        SingleLineText,
        MultiLineText,
        SingleLineSecret,
        MultiLineSecret,
        CodeBlock,
        Number,
        Duration,
        FilePath,
        List
    }
}
