/**
 * 
 */
package zachary.zhanghao.columnar.hbase;

import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import zachary.zhanghao.columnar.exception.ColumnarClientException;
import zachary.zhanghao.columnar.funciton.base.HBaseSourceAware;
import zachary.zhanghao.columnar.funciton.base.Schema;

/**
 * @author zachary.zhang
 *
 */
@Slf4j
public class HBaseColumnarAdmin implements Schema, HBaseSourceAware {

    private HBaseSource source;

    @Override
    public void createTable(String tableName, String... columnFamily)
                    throws ColumnarClientException {

        try (HBaseAdmin admin = this.source.getHBaseAdmin()) {// auto close hbaseadmin
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : columnFamily) {
                tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(cf)));
            }

            // 添加协处理器
            // tableDesc.addCoprocessor(className, jarFilePath, priority, kvs);
            admin.createTable(tableDesc);

            log.info("create table:{} success", tableName);
        } catch (Exception e) {
            log.error("some error occured in createTable method:", e);
            throw new ColumnarClientException(e);
        }
    }

    @Override
    public void deleteTable(String tableName) throws ColumnarClientException {
        try (HBaseAdmin admin = this.source.getHBaseAdmin()) {// auto close hbaseadmin

            admin.deleteTable(tableName);
            log.info("delete table:{} success", tableName);
        } catch (Exception e) {
            log.error("some error occured in createTable method:", e);
            throw new ColumnarClientException(e);
        }
    }


    @Override
    public HBaseSource getHBaseSource() {
        return this.source;
    }

    @Override
    public void setHBaseSource(HBaseSource source) {
        this.source = source;
    }

}
