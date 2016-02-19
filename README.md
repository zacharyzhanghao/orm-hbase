# orm-hbase
an ORM framework for operating hbase easily. It worked like Hibernate,you can just include the jar and annotate your PO(persistent object),it will works well.</br>

### some features:
1. annotate the PO with @Table/@Column/@RowKey, it can automatically mapping your hbase table
2. supply CRUD function:create,research,update,delete
3. supply query data by page(pagination)

one word,you can use HBase like use hibernate.


demo:</br>
1.just annotate the @Table,@RowKey,@Column in your pojo

  @Table(name = "user")
  public class User {
  
      @RowKey
      private int id;
  
      @Column(family = "info")
      private int userId;
  
      @Column(family = "info", name = "user_name1")
      private String userName;
  
      @Column(family = "info")
      private long age;
  }

2.use HBaseColumnarClient instance to handle the po,like below code</br>

      HBaseColumnarClient client = new HBaseColumnarClient(scanCaching, scanBatch);
      DataSourceConfig config = new DataSourceConfig("hbase.properties");
      HBaseSource source = new HBaseSource(config.getProperties());
      client.setHBaseSource(source);
      client.putObject(user);
