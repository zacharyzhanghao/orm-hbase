/**
 * 
 */
package zachary.zhanghao.columnar.demo;

import java.util.List;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import zachary.zhanghao.columnar.config.DataSourceConfig;
import zachary.zhanghao.columnar.exception.ColumnarClientException;
import zachary.zhanghao.columnar.hbase.Criteria;
import zachary.zhanghao.columnar.hbase.HBaseColumnarClient;
import zachary.zhanghao.columnar.hbase.HBaseSource;
import zachary.zhanghao.columnar.hbase.PageBean;

import com.google.common.collect.Lists;

/**
 * @author zachary.zhang
 *
 */
public class CriteriasDemo {


    public static void main(String[] args) throws ColumnarClientException {


        HBaseColumnarClient client = constructClient();

        // insert data
        int id = 98765;
        User user = new User();
        user.setAge(12L);
        user.setId(id);
        user.setUserName("zhanghao");
        Criteria.put(User.class).persistenObject(user).build().excute(client);


        byte[] startRow = Bytes.toBytes(10);
        byte[] endRow = Bytes.toBytes(21);

        // count the data
        Filter[] filters = null;
        long count =
                        Criteria.aggregate(User.class).fromRow(startRow).toRow(endRow)
                                        .filters(filters).build().count(client);

        System.out.println("count:" + count);

        // sum the column value
        long sum =
                        Criteria.aggregate(User.class).fromRow(startRow).toRow(endRow)
                                        .filters(filters).propertyName("age").build().sum(client);
        System.out.println("sum:" + sum);

        // query by rowKey
        User queryUser =
                        Criteria.find(User.class).byRowKey(Bytes.toBytes(id)).build().query(client);
        if (queryUser != null) {
            System.out.println(queryUser.getUserName());
        }

        // query rowkey list
        List<byte[]> rowKeyList = Lists.newArrayList();
        rowKeyList.add(Bytes.toBytes(1));
        rowKeyList.add(Bytes.toBytes(2));
        rowKeyList.add(Bytes.toBytes(3));
        rowKeyList.add(Bytes.toBytes(4));
        rowKeyList.add(Bytes.toBytes(5));
        List<User> queryUserList =
                        Criteria.find(User.class).byRowKeyList(rowKeyList).build()
                                        .queryList(client);
        for (User u : queryUserList) {
            System.out.println(u.getId());
        }

        // query from startRow to endRow
        List<User> queryList =
                        Criteria.find(User.class).fromRow(startRow).toRow(endRow).build()
                                        .queryList(client);
        for (User u : queryList) {
            System.out.println(u.getId());
        }

        // query by page
        PageBean<User> pageBean = new PageBean<User>() {};
        pageBean.setPageSize(10);
        pageBean.setStartRow(startRow);
        pageBean.setStopRow(endRow);
        PageBean<User> queryPage =
                        Criteria.find(User.class).pageBean(pageBean).build().queryPage(client);

        List<User> dataSet = queryPage.getDataSet();
        for (User u : dataSet) {
            System.out.println(u.getId());
        }

        // delete data
        byte[] rowKey = Bytes.toBytes(id);
        Criteria.delete(User.class).byRowKey(rowKey).build().excute(client);
    }

    private static HBaseColumnarClient constructClient() throws ColumnarClientException {
        int scanCaching = 200;
        int scanBatch = 100;
        HBaseColumnarClient client = new HBaseColumnarClient(scanCaching, scanBatch);

        DataSourceConfig config = new DataSourceConfig("hbase.properties");

        HBaseSource source = new HBaseSource(config.getProperties());

        client.setHBaseSource(source);
        return client;
    }
}
