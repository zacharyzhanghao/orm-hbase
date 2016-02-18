/**
 * 
 */
package com.testbird.artisan.columnar.type;

import java.lang.reflect.ParameterizedType;

import com.google.common.base.Preconditions;

/**
 * judge if the accept type is match generic type
 * 
 * @author zachary.zhang
 *
 */
public abstract class TypeResolverBase<T> implements TypeResolver<T> {

    public boolean accept(Object o) {
        Preconditions.checkNotNull(o, "target object can't be null");

        // getActualTypeArguments only can get the father generic type
        // so ,it need to extend this class and get the generic type from the sub class
        Class<?> type =
                        (Class<?>) ((ParameterizedType) getClass().getGenericSuperclass())
                                        .getActualTypeArguments()[0];

        return o.getClass() == type;
    }
}
