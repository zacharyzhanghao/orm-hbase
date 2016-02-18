/**
 * 
 */
package com.testbird.artisan.columnar.funciton.base;

import com.testbird.artisan.columnar.hbase.HBaseSource;

/**
 * 提供对HBaseSource的操作功能
 * 
 * @author zachary.zhang
 *
 */
public interface HBaseSourceAware {

    HBaseSource getHBaseSource();

    void setHBaseSource(HBaseSource source);

}
