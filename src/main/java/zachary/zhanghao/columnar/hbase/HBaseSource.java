/**
 * 
 */
package zachary.zhanghao.columnar.hbase;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

import zachary.zhanghao.columnar.exception.ColumnarClientException;

/**
 * hbase的资源类
 * 
 * @author zachary.zhang
 *
 */
public class HBaseSource {

    private Configuration conf;
    private HConnection connection;
    private Properties configProperties;


    public HBaseSource(Properties configProperties) throws ColumnarClientException {
        this.configProperties = configProperties;
        init();
    }

    /**
     * 初始化hbase,加载配置信息
     * 
     * <pre>
     * 修改读取property的方式，从hbase-site.xml --> hbase.properties
     * <P>
     * 如果有需要，请添加以下参数 :
     * <li>hbase.client.write.buffer 2097152
     * <li>hbase.client.retries.number 35
     * <li>hbase.client.scanner.caching 100
     * <li>hbase.client.keyvalue.maxsize 10485760
     * <li>timeout 等
     * <P>
     * 以上参数都为hbase默认参数，具体可以从hbase-site.xml获取
     * @throws ColumnarClientException
     */
    public void init() throws ColumnarClientException {
        try {

            conf = HBaseConfiguration.create();
            for (Object key : configProperties.keySet()) {
                conf.set(key.toString(), configProperties.getProperty(key.toString()));
            }
            connection = HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            throw new ColumnarClientException(e);
        }
    }


    public HConnection getConn() throws IOException {
        if (connection == null || connection.isClosed()) {
            connection = HConnectionManager.createConnection(conf);
        }
        return connection;
    }

    /**
     * 根据table name 返回HBase HTableInterface对象，可以执行CRUD操作 使用完毕之后，需要调用HTableInterface.close()
     * 
     * @param tableName
     * @return HTableInterface
     * @throws ColumnarClientException
     */
    public HTableInterface getTable(String tableName) throws ColumnarClientException {
        try {
            return getConn().getTable(tableName);
        } catch (IOException e) {
            throw new ColumnarClientException(e);
        }
    }

    public HBaseAdmin getHBaseAdmin() throws ColumnarClientException {
        try {
            return new HBaseAdmin(connection);
        } catch (MasterNotRunningException | ZooKeeperConnectionException e) {
            throw new ColumnarClientException(e);
        }
    }

    /**
     * 关闭table
     * 
     * @param table
     * @throws ColumnarClientException
     */
    public void closeTable(HTableInterface table) throws ColumnarClientException {
        if (table == null) {
            return;
        }
        try {
            table.close();
        } catch (IOException e) {
            throw new ColumnarClientException(e);
        }
    }

    public Configuration getConfiguration() {
        return this.conf;
    }
}
