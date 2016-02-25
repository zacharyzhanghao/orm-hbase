/**
 * 
 */
package zachary.zhanghao.columnar.funciton.base;

import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;

import zachary.zhanghao.columnar.exception.ColumnarClientException;
import zachary.zhanghao.columnar.hbase.PageBean;

/**
 * 提供hbase基本功能，比如CRUD
 * 
 * @author zachary.zhang
 *
 */
public interface Persistent {

    <T> void putObject(T po) throws ColumnarClientException;

    <T> void putObjectList(List<T> poList) throws ColumnarClientException;

    <T> T findObject(byte[] rowKey, Class<T> type) throws ColumnarClientException;

    <T> List<T> findObjectList(List<byte[]> rowKeyList, Class<T> type)
                    throws ColumnarClientException;

    <T> List<T> findObjectList(byte[] startRow, byte[] endRow, Class<T> type)
                    throws ColumnarClientException;

    <T> PageBean<T> findObjectListByPage(PageBean<T> pageBean) throws ColumnarClientException;

    <T> PageBean<T> findObjectListByPage(PageBean<T> pageBean, Filter... filters)
                    throws ColumnarClientException;

    void deleteObject(byte[] rowKey, Class<?> po) throws ColumnarClientException;

    void deleteObjectList(List<byte[]> rowKeyList, Class<?> po) throws ColumnarClientException;

}
