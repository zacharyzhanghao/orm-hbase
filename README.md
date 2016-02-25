# orm-hbase
an ORM framework for operating hbase easily. It worked like Hibernate,you can just include the jar and annotate your PO(persistent object),it will works well.</br>

### some features:
1. annotate the PO with @Table/@Column/@RowKey, it can automatically mapping your hbase table
2. supply CRUD function:create,research,update,delete
3. supply query data by page(pagination)
4. supply Criteria style to operate hbase like hibernate Criteria

one word,you can use HBase like use hibernate.


### Demo:
only 2 steps to master this framwork</br></br>
1.just annotate the @Table,@RowKey,@Column in your pojo</br>

     @Table(name = "user")</br>
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

3.Criteria style to operat hbase like hibernate criteria </br>
      // count the data
        Filter[] filters = null;
        long count =
                        Criteria.aggregate(User.class).fromRow(startRow).toRow(endRow)
                                        .filters(filters).build().count(client);
                                        
      // sum the column value
        long sum =
                        Criteria.aggregate(User.class).fromRow(startRow).toRow(endRow)
                                        .filters(filters).propertyName("age").build().sum(client);
                                        
     // query by rowKey
        User queryUser =
                        Criteria.find(User.class).byRowKey(Bytes.toBytes(id)).build().query(client);
                        
     // query from startRow to endRow
        List<User> queryList =
                        Criteria.find(User.class).fromRow(startRow).toRow(endRow).build()
                                        .queryList(client);
                                        
     // query by page
        PageBean<User> pageBean = new PageBean<User>() {};
        pageBean.setPageSize(10);
        pageBean.setStartRow(startRow);
        pageBean.setStopRow(endRow);
        PageBean<User> queryPage =
                        Criteria.find(User.class).pageBean(pageBean).build().queryPage(client);
                        
      // delete data
        byte[] rowKey = Bytes.toBytes(id);
        Criteria.delete(User.class).byRowKey(rowKey).build().excute(client); 
