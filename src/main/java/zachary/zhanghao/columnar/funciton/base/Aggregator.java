/**
 * 
 */
package zachary.zhanghao.columnar.funciton.base;

import org.apache.hadoop.hbase.filter.Filter;

import zachary.zhanghao.columnar.exception.ColumnarClientException;

/**
 * 提供hbase的聚合计算功能
 * 
 * @author zachary.zhang
 *
 */
public interface Aggregator {

    /**
     * 求行数。求表中指定范围数据的行数。
     * 
     * <pre>
     * 注意:必须在创建表的时指定coprocessor:org.apache.hadoop.hbase.coprocessor.AggregateImplementation
     * ,否则无法运行
     * 
     * @param startRow
     * @param endRow
     * @param po
     * @return
     * @throws ColumnarClientException
     */
    long count(byte[] startRow, byte[] endRow, Class<?> po) throws ColumnarClientException;

    /**
     * 求行数。求表中指定范围数据的行数。
     * 
     * <pre>
     * 注意:必须在创建表的时指定coprocessor:org.apache.hadoop.hbase.coprocessor.AggregateImplementation
     * ,否则无法运行
     * @param startRow
     * @param endRow
     * @param po
     * @param filter
     * @return
     * @throws ColumnarClientException
     */
    long count(byte[] startRow, byte[] endRow, Class<?> po, Filter... filter)
                    throws ColumnarClientException;

    /**
     * 求和。求指定column的sum值.只支持long类型的column
     * 
     * <pre>
     * 注意:必须在创建表的时指定coprocessor:org.apache.hadoop.hbase.coprocessor.AggregateImplementation
     * ,否则无法运行
     * 
     * @param startRow
     * @param endRow
     * @param po
     * @param propertyName po对象的field名称
     * @return
     * @throws ColumnarClientException
     */
    long sum(byte[] startRow, byte[] endRow, Class<?> po, String propertyName)
                    throws ColumnarClientException;

    /**
     * 求和。求指定column的sum值.只支持long类型的column
     * 
     * <pre>
     * 注意:必须在创建表的时指定coprocessor:org.apache.hadoop.hbase.coprocessor.AggregateImplementation
     * ,否则无法运行
     * 
     * @param startRow
     * @param endRow
     * @param po
     * @param propertyName
     * @param filter
     * @return
     * @throws ColumnarClientException
     */
    public long sum(byte[] startRow, byte[] endRow, Class<?> po, String propertyName,
                    Filter... filter) throws ColumnarClientException;
}
