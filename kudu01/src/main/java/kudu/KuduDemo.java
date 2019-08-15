package kudu;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @author RanMoAnRan
 * @ClassName: KuduDemo
 * @projectName DMP-Project
 * @description: TODO
 * @date 2019/8/8 22:02
 */
@SuppressWarnings("ALL")
public class KuduDemo {
    // 对于Kudu操作，获取KuduClient客户端实例对象
    KuduClient kuduClient = null;

    /**
     * 初始化KuduClient实例对象
     */
    @Before
    public void init() {
        String masterAddresses = "hadoop01:7051,hadoop02:7051,hadoop03:7051";
        // 构建KuduClient实例对象
        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
                // 设置超时时间间隔，默认值为10s
                .defaultSocketReadTimeoutMs(6000)
                // 采用建造者模式构建实例对象
                .build();
    }


    /**
     * 操作结束，关闭KuduClient
     */
    @After
    public void clean() throws KuduException {
        // 关闭KuduClient对象，释放资源
        if (null != kuduClient) kuduClient.close();
    }


    /**
     * 用于构建Kudu表中每列的字段信息Schema
     *
     * @param name  字段名称
     * @param type  字段类型
     * @param isKey 是否为Key
     * @return ColumnSchema对象
     */
    private ColumnSchema newColunmnSchema(String name, Type type, boolean isKey) {
        // 创建ColumnSchemaBuilder实例对象
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        // 设置是否为主键
        column.key(isKey);
        // 构建	ColumnSchema
        return column.build();
    }


    /**
     * 创建Kudu中的表，表的结构如下所示：
     * create table itcast_user(
     * id int,
     * name string,
     * age byte,
     * primary key(id)
     * )
     * paritition by hash(id) partitions 3
     * stored as kudu ;
     */
    @Test
    public void createKuduTable() throws KuduException {
        // a. 构建表的Schema信息
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        // public ColumnSchemaBuilder(String name, Type type)
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columnSchemas.add(newColunmnSchema("name", Type.STRING, false));
        columnSchemas.add(newColunmnSchema("age", Type.INT32, false));
        // 定义Schema信息
        Schema schema = new Schema(columnSchemas);

        // b. Kudu表的分区策略及分区副本数目设置
        CreateTableOptions tableOptions = new CreateTableOptions();
        // 哈希分区
        List<String> columns = new ArrayList<String>();
        columns.add("id"); // 按照id进行分区操作
        tableOptions.addHashPartitions(columns, 3); // 设置三个分区数buckets

        // 副本数设置
        // illegal replication factor 2 (replication factor must be odd)
        tableOptions.setNumReplicas(1);

		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
        // c. 在Kudu中创建表
        KuduTable userTable = kuduClient.createTable("itcast_users", schema, tableOptions);
        System.out.println(userTable.toString());
    }


    /**
     * 将数据插入到Kudu Table中： INSERT INTO (id, name, age) VALUES (1001, "zhangsan", 26)
     */
    @Test
    public void insertKuduTable() throws KuduException {

        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users");

        // b. 获取KuduSession，用于对集群进行交互，比如表的CRUD操作
        KuduSession kuduSession = kuduClient.newSession();

        // TODO: 设置 手动刷新数据到表中
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        // 设置缓存大小
        kuduSession.setMutationBufferSpace(3000);

        // 产生随机数
        Random random = new Random();
        for (int index = 0; index < 100; index++) {
            // c. 构建Insert实例对象，封装插入的每条数据，类似HBase中Put实例对象
            Insert insert = kuduTable.newInsert();
            // 设置每条数据Row
            PartialRow partialRow = insert.getRow();
            partialRow.addInt("id", 1 + index);
            partialRow.addString("name", "zhangsan-" + index);
            partialRow.addInt("age", 18 + random.nextInt(10));

            // d. 插入数据到表中
            kuduSession.apply(insert);
        }
        // TODO: 刷新数据到表中
        kuduSession.flush();

        // e. 关闭Session会话
        kuduSession.close();
    }


    /**
     * 从Kudu Table中查询数据
     */
    @Test
    public void selectKuduTable() throws KuduException {
        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users");

        // b. 构建KuduScanner扫描器，进行查询数据，类似HBase中Scan类
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        KuduScanner kuduScanner = scannerBuilder.build();

        int batchTime = 1;
        // c. 迭代获取数据, 分批次(每个分区查询一次，封装在一起)返回查询的数据，判断是否有数据
        while (kuduScanner.hasMoreRows()) {
            System.out.println("Batch = " + batchTime++);
            // 获取返回中某批次数据，有多条数据，以迭代器形式返回
            RowResultIterator rowResults = kuduScanner.nextRows();
            // 从迭代器中获取每条数据
            while (rowResults.hasNext()) {
                RowResult rowResult = rowResults.next();
                System.out.println(
                        "id = " + rowResult.getInt("id") +
                                ", name = " + rowResult.getString("name") +
                                ", age = " + rowResult.getInt("age")
                );
            }
        }
    }


    /**
     * 设置过滤：比如只查询id和age两个字段的值，年龄age小于25，id大于50
     */
    @Test
    public void selectrangeKuduTable() throws KuduException {
        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users");

        // b. 构建KuduScanner扫描器，进行查询数据，类似HBase中Scan类
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);

        // TODO: 设置过滤：比如只查询id和age两个字段的值，年龄age小于25，id大于50
        // i. 设置获取指定列的字段值
        List<String> columnNames = new ArrayList<String>();
        columnNames.add("age");
        columnNames.add("name");
        columnNames.add("id");
        // ii. 设置age 小于25
        KuduPredicate predicateAge = KuduPredicate.newComparisonPredicate(
                newColunmnSchema("age", Type.INT32, false), //
                KuduPredicate.ComparisonOp.LESS_EQUAL, //
                25
        );
        // iii. id大于250
        KuduPredicate predicateId = KuduPredicate.newComparisonPredicate(
                newColunmnSchema("id", Type.INT32, true), //
                KuduPredicate.ComparisonOp.GREATER, //
                50
        );
        scannerBuilder
                .addPredicate(predicateAge)
                .addPredicate(predicateId)
                .setProjectedColumnNames(columnNames);
        KuduScanner kuduScanner = scannerBuilder.build();

        int batchTime = 1;
        // c. 迭代获取数据, 分批次(每个分区查询一次，封装在一起)返回查询的数据，判断是否有数据
        while (kuduScanner.hasMoreRows()) {
            System.out.println("Batch = " + batchTime++);
            // 获取返回中某批次数据，有多条数据，以迭代器形式返回
            RowResultIterator rowResults = kuduScanner.nextRows();
            // 从迭代器中获取每条数据
            while (rowResults.hasNext()) {
                RowResult rowResult = rowResults.next();
                System.out.println(
                        "id = " + rowResult.getInt("id") +
                                ", name = " + rowResult.getString("name") +
                                ", age = " + rowResult.getInt("age")
                );
            }
        }
    }


    /**
     * 更新Kudu Table表中的数据
     */
    @Test
    public void updateKuduTable() throws KuduException {
        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users");

        // b. 获取KuduSession，用于对集群进行交互，比如表的CRUD操作
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        // c. 更新数据，获取Update实例对象
        Update update = kuduTable.newUpdate();

        PartialRow partialRow = update.getRow();
        partialRow.addInt("id", 51);
        partialRow.addString("name", "lisi-update");

        // d. 插入数据到表中
        kuduSession.apply(update);
        kuduSession.flush();

        // e. 关闭Session会话
        kuduSession.close();
    }


    /**
     * 对Kudu Table表中的数据插入更新（如果主键不存在插入数据，存在更新数据）
     */
    @Test
    public void upsertKuduTable() throws KuduException {
        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users");

        // b. 获取KuduSession，用于对集群进行交互，比如表的CRUD操作
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        // c. 获取Upsert实例对象
        Upsert upsert = kuduTable.newUpsert();

        PartialRow upsertRow = upsert.getRow();
        upsertRow.addInt("id", 3001);
        upsertRow.addString("name", "wangwu-upsert");
        upsertRow.addInt("age", 23);

        // d. 插入数据到表中
        kuduSession.apply(upsert);
        kuduSession.flush();

        // e. 关闭Session会话
        kuduSession.close();
    }


    /**
     * 删除Kudu Table表中的数据
     */
    @Test
    public void deleteKuduTable() throws KuduException {
        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users");

        // b. 获取KuduSession，用于对集群进行交互，比如表的CRUD操作
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        // c. 依据表的主键删除数据
        Delete delete = kuduTable.newDelete();

        PartialRow deleteRow = delete.getRow();
        deleteRow.addInt("id", 3001);

        // d. 插入数据到表中
        kuduSession.apply(delete);
        kuduSession.flush();

        // e. 关闭Session会话
        kuduSession.close();
    }


    /**
     * 删除Kudu中的表
     */
    @Test
    public void dropKuduTable() throws KuduException {
        // 判断表是否存在，如果存在即删除
        if (kuduClient.tableExists("itcast_users")) {
            DeleteTableResponse response = kuduClient.deleteTable("itcast_users");
            System.out.println(response.getElapsedMillis());
        }
    }


    /**
     * 创建Kudu中的表，采用范围分区策略
     * Range分区的方式：id
     * <p>
     * - < 100
     * - 100 - 500
     * - 500 <=
     */
    @Test
    public void createKuduTableHash() throws KuduException {
        // a. 构建表的Schema信息
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        // public ColumnSchemaBuilder(String name, Type type)
        columnSchemas.add(newColunmnSchema("id", Type.INT32, true));
        columnSchemas.add(newColunmnSchema("name", Type.STRING, false));
        columnSchemas.add(newColunmnSchema("age", Type.INT32, false));
        // 定义Schema信息
        Schema schema = new Schema(columnSchemas);

        // b. Kudu表的分区策略及分区副本数目设置
        CreateTableOptions tableOptions = new CreateTableOptions();

        // 哈希分区
        List<String> columns = new ArrayList<String>();
        columns.add("id"); // 按照id进行分区操作
        tableOptions.addHashPartitions(columns, 4); // 设置四个分区数bucket

        // 副本数设置
        // illegal replication factor 2 (replication factor must be odd)
        tableOptions.setNumReplicas(1);

        // c. 在Kudu中创建表
        KuduTable userTable = kuduClient.createTable("itcast_users_hash", schema, tableOptions);
        System.out.println(userTable.toString());
    }


    /**
     * 创建Kudu中的表，采用范围分区策略
     * Range分区的方式：id
     * <p>
     * - < 100
     * - 100 - 500
     * - 500 <=
     */
    @Test
    public void createKuduTableRange() throws KuduException {
        // a. 构建表的Schema信息
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        // public ColumnSchemaBuilder(String name, Type type)
        columnSchemas.add(newColunmnSchema("id", Type.INT32, true));
        columnSchemas.add(newColunmnSchema("name", Type.STRING, false));
        columnSchemas.add(newColunmnSchema("age", Type.INT32, false));
        // 定义Schema信息
        Schema schema = new Schema(columnSchemas);

        // b. Kudu表的分区策略及分区副本数目设置
        CreateTableOptions tableOptions = new CreateTableOptions();
        // 哈希分区
        List<String> columns = new ArrayList<String>();
        columns.add("id");
        // TODO: 按照id进行范围分区操作，分区键必须是主键 或 主键的一部分
        tableOptions.setRangePartitionColumns(columns);
        // TODO: 设置分区范围，类似HBase中表的Region预分区操作
        /**
         * value < 100
         * 100 <= value < 500
         * 500 <= value
         */
        // value < 100
        PartialRow upper = new PartialRow(schema);
        upper.addInt("id", 100);
        // 添加分区范围
        tableOptions.addRangePartition(new PartialRow(schema), upper);

        // 100 <= value < 500
        PartialRow lower100 = new PartialRow(schema);
        lower100.addInt("id", 100);
        PartialRow upper500 = new PartialRow(schema);
        upper500.addInt("id", 500);
        // 添加分区范围
        tableOptions.addRangePartition(lower100, upper500);

        // 500 <= value
        PartialRow lower500 = new PartialRow(schema);
        lower500.addInt("id", 500);
        // 添加分区范围
        tableOptions.addRangePartition(lower500, new PartialRow(schema));


        // 副本数设置
        // illegal replication factor 2 (replication factor must be odd)
        tableOptions.setNumReplicas(1);

        // c. 在Kudu中创建表
        KuduTable userTable = kuduClient.createTable("itcast_users_range", schema, tableOptions);
        System.out.println(userTable.toString());
    }


    /**
     * 创建Kudu中的表，采用多级分区策略，结合哈希分区和范围分区组合使用
     * - id 做 hash分区，分5区
     * - age 做 range分区，分3个区
     * - < 21（小于等于20岁）
     * - 21 -  41（21岁到40岁）
     * - 41（41岁以上，涵盖41岁）
     * - 数据一共分了 15个区
     */
    @Test
    public void createKuduTableMulti() throws KuduException {
        // a. 构建表的Schema信息
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        columnSchemas.add(newColunmnSchema("id", Type.INT32, true));
        columnSchemas.add(newColunmnSchema("age", Type.INT32, true));
        columnSchemas.add(newColunmnSchema("name", Type.STRING, false));
        // 定义Schema信息
        Schema schema = new Schema(columnSchemas);

        // b. Kudu表的分区策略及分区副本数目设置
        CreateTableOptions tableOptions = new CreateTableOptions();

        // TODO： e.1. 设置哈希分区
        List<String> columnsHash = new ArrayList<String>();
        columnsHash.add("id");
        tableOptions.addHashPartitions(columnsHash, 5);

        // TODO: e.2. 设值范围分区
		/*
		  age 做 range分区，分3个区**
			- < 21（小于等于20岁）
			- 21 -  41（21岁到40岁）
			- 41（41岁以上，涵盖41岁）
		 */
        List<String> columnsRange = new ArrayList<String>();
        columnsRange.add("age");
        tableOptions.setRangePartitionColumns(columnsRange);
        // 添加范围分区
        PartialRow upper21 = new PartialRow(schema);
        upper21.addInt("age", 21);
        tableOptions.addRangePartition(new PartialRow(schema), upper21);

        // 添加范围分区
        PartialRow lower21 = new PartialRow(schema);
        lower21.addInt("age", 21);
        PartialRow upper41 = new PartialRow(schema);
        upper41.addInt("age", 41);
        tableOptions.addRangePartition(lower21, upper41);

        // 添加范围分区
        PartialRow lower41 = new PartialRow(schema);
        lower41.addInt("age", 41);
        tableOptions.addRangePartition(lower41, new PartialRow(schema));

        // 副本数设置
        tableOptions.setNumReplicas(1);

        // c. 在Kudu中创建表
        KuduTable userTable = kuduClient.createTable("itcast_users_multi", schema, tableOptions);
        System.out.println(userTable.toString());
    }

}
