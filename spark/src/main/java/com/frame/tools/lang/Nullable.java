package com.frame.tools.lang;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;

/**
 * 变量、参数 、返回值不为空说明
 * @author XiaShuai on 2020/4/8.
 */
@Documented
@Retention(RetentionPolicy.CLASS)
@Target({PARAMETER, FIELD, METHOD})
public @interface Nullable {
}
