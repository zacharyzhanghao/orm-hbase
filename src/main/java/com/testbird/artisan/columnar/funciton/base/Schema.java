/**
 * 
 */
package com.testbird.artisan.columnar.funciton.base;

import com.testbird.artisan.columnar.exception.ColumnarClientException;

/**
 * 提供HBase schema的操作功能
 * 
 * @author zachary.zhang
 *
 */
public interface Schema {

    void createTable(String tableName, String... columnFamily) throws ColumnarClientException;

    void deleteTable(String tableName) throws ColumnarClientException;
}
