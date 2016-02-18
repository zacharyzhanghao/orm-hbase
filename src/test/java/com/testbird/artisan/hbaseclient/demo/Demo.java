/**
 * 
 */
package com.testbird.artisan.hbaseclient.demo;

import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;

import com.testbird.artisan.columnar.config.DataSourceConfig;
import com.testbird.artisan.columnar.exception.ColumnarClientException;
import com.testbird.artisan.columnar.hbase.HBaseColumnarAdmin;
import com.testbird.artisan.columnar.hbase.HBaseColumnarClient;
import com.testbird.artisan.columnar.hbase.HBaseSource;
import com.testbird.artisan.columnar.hbase.PageBean;

/**
 * 
 * @author zachary.zhang
 *
 */
public class Demo {

    public static void main(String[] args) {
        try {
            HBaseColumnarClient client = constructClient();

            putObject(client);

            findObject(client);

            findObjectRange(client);

            findObjectListByPage(client);

            client.deleteObject(Bytes.toBytes(1), User.class);

            long count = client.count(null, null, User.class);
            System.out.println(count + " rows data in user table");

            long sum = client.sum(null, null, User.class, "age");
            System.out.println("the sum of age column value is  :" + sum);


            // create table
            HBaseColumnarAdmin hbaseAdmin = constructAdmin();
            hbaseAdmin.createTable("hbase_client", "t", "t1", "t2");

            // delete table
            hbaseAdmin.deleteTable("hbase_client");



        } catch (ColumnarClientException e) {
            e.printStackTrace();
        }
    }

    private static void findObjectListByPage(HBaseColumnarClient client)
                    throws ColumnarClientException {
        PageBean<User> pageBean = new PageBean<User>() {};
        pageBean.setStartRow(Bytes.toBytes(1));
        pageBean.setStopRow(Bytes.toBytes(20));
        pageBean.setPageSize(10);

        while ((pageBean = client.findObjectListByPage(pageBean)).getDataSet().size() > 0) {

            // List<User> dataSet = pageBean.getDataSet();
            System.out.println("current page is:" + pageBean.getCurrentPage() + ", data size:"
                            + pageBean.getDataSet().size());
        }
    }

    private static void findObjectRange(HBaseColumnarClient client) throws ColumnarClientException {
        List<User> findObjectList =
                        client.findObjectList(Bytes.toBytes(1), Bytes.toBytes(4), User.class);

        System.out.println("the users count is:" + findObjectList.size());
    }

    private static void findObject(HBaseColumnarClient client) throws ColumnarClientException {
        User findObject = client.findObject(Bytes.toBytes(1), User.class);
        System.out.println(findObject.getUserName());
        System.out.println(findObject.getId());
    }

    private static void putObject(HBaseColumnarClient client) throws ColumnarClientException {

        for (int i = 1; i < 21; i++) {
            User user = new User();
            user.setId(i);
            user.setUserId(123);
            // user.setUserName("zhangsan");
            user.setAge(25L);
            client.putObject(user);
        }
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

    public static HBaseColumnarAdmin constructAdmin() throws ColumnarClientException {
        HBaseColumnarAdmin adminClient = new HBaseColumnarAdmin();
        DataSourceConfig config = new DataSourceConfig("hbase.properties");
        HBaseSource source = new HBaseSource(config.getProperties());

        adminClient.setHBaseSource(source);
        return adminClient;
    }
}
