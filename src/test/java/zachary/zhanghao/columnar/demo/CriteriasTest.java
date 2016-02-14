/**
 * 
 */
package zachary.zhanghao.columnar.demo;

import zachary.zhanghao.columnar.config.DataSourceConfig;
import zachary.zhanghao.columnar.exception.ColumnarClientException;
import zachary.zhanghao.columnar.funciton.ColumnarClient;
import zachary.zhanghao.columnar.hbase.Criterias;
import zachary.zhanghao.columnar.hbase.HBaseColumnarClient;
import zachary.zhanghao.columnar.hbase.HBaseSource;

/**
 * @author zachary.zhang
 *
 */
public class CriteriasTest {


    public static void main(String[] args) throws ColumnarClientException {

        ColumnarClient client = constructClient();

        User user = new User();
        user.setAge(12L);
        user.setId(7891);
        user.setUserName("zhanghao");
        Criterias.put(User.class).persistenObject(user).build().excute(client);
        //
        // byte[] rowKey = null;
        // Criterias.delete(User.class).byRowKey(rowKey).build().excute(client);
        //
        // byte[] startRow = null;
        // byte[] endRow = null;
        // Filter[] filters = null;
        // Criterias.aggregate(User.class).startRow(startRow).endRow(endRow).filters(filters).build()
        // .count(client);
        //
        // Criterias.aggregate(User.class).startRow(startRow).endRow(endRow).filters(filters)
        // .propertyName("user_name").build().sum(client);
        //
        // User queryUser = Criterias.find(User.class).byRowKey(rowKey).build().query(client);
        //
        // List<byte[]> rowKeyList = Lists.newArrayList();
        // List<User> queryUserList =
        // Criterias.find(User.class).byRowKeyList(rowKeyList).build().query(client);
        //
        // List<User> queryList =
        // Criterias.find(User.class).startRow(startRow).endRow(endRow).build()
        // .queryList(client);
        //
        // PageBean<User> pageBean = new PageBean<User>() {};
        // PageBean<User> queryPage =
        // Criterias.find(User.class).pageBean(pageBean).build().queryPage(client);

    }

    private static ColumnarClient constructClient() throws ColumnarClientException {
        int scanCaching = 200;
        int scanBatch = 100;
        ColumnarClient client = new HBaseColumnarClient(scanCaching, scanBatch);

        DataSourceConfig config = new DataSourceConfig("hbase.properties");

        HBaseSource source = new HBaseSource(config.getProperties());

        client.setHBaseSource(source);
        return client;
    }
}
