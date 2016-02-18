/**
 * 
 */
package com.testbird.artisan.columnar.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * hbase中用以指定tableName
 * 
 * @author zachary.zhang
 *
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Table {

    /**
     * 映射到hbase中的tableName
     * 
     * @return
     */
    String name() default "";


}
