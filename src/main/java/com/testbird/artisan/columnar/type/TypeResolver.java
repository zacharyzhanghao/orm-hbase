/**
 * 
 */
package com.testbird.artisan.columnar.type;



/**
 * 类型转换接口
 * 
 * @author zachary.zhang
 *
 */
public interface TypeResolver<T> {

    /**
     * transform object to bytes
     * 
     * @param object
     * @return
     */
    byte[] toBytes(T object);

    /**
     * transform bytes to object
     * 
     * @param bytes
     * @return
     */
    T toObject(byte[] bytes);

    /**
     * judge if the accept type is match the generic type
     * 
     * @param o
     * @return true,if match
     */
    boolean accept(Object o);
}
