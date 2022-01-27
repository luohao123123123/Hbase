package HbaseApi.daoImp;

import HbaseApi.dao.HbaseApi_Dao;
import HbaseApi.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class HbaseApi_DaoImp implements HbaseApi_Dao {
    //todo:1.构建Configuration, Connection, Admin
    //Configuration 持有了zk的信息，进而hbase集群的信息可以间接获得
    //Connection  hbase连接  借助配置信息 获得连接
    private static final Connection connection;
    private static Admin admin;

    //todo:为静态属性初始化，或者说辅助类初始化
    static {
        connection=HbaseUtil.getConnection();
        try {
            assert connection != null;
            admin=connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //TODO:创建库
    @Override
    public void createNS(String namespace) throws IOException {
        //①构建  ns的描述器  声明库名
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
        //②创建库
        try{
            admin.createNamespace(namespaceDescriptor);
        }catch (NamespaceExistException e){
            System.out.println("该库已经存在！");
        }
        //③关资源
        admin.close();
    }

    //TODO:判断表是否存在
    @Override
    public boolean isExists(String tableName) throws IOException {
        boolean exists = admin.tableExists(TableName.valueOf(tableName));
        System.out.println("exits:" + exists);
        admin.close();
        return exists;
    }

    @Override
    //TODO:创建表                             String...  表示多个参数
    public void createTable(String tableName, String... info) throws IOException {

        //①HTableDescriptor
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //②添加columnFamily 列族
        for (String cf : info) {
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
        }
        //③建表
        admin.createTable(hTableDescriptor);
        //④释放资源
        admin.close();

    }

    //TODO:禁用并删除表
    @Override
    public void deleteTable(String tableName) throws IOException {
        admin.disableTable(TableName.valueOf(tableName));
        admin.deleteTable(TableName.valueOf(tableName));
        admin.close();
    }

    //TODO:插入数据
    @Override
    public void insertData(String tableName, String rowkey, String column, String value) throws IOException {
        //①获取table
        Table table = connection.getTable(TableName.valueOf(tableName));
        //②获得put
        Put put = new Put(Bytes.toBytes(rowkey));//把String类型转成bytes类型
        put.addColumn(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]),
                Bytes.toBytes(value));
        table.put(put); //③添加数据
        table.close();//④释放资源
    }

    //TODO:删除一行或多行数据
    @Override
    public void deleteData(String tableName, String... rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        for (String rk : rowkey) {
            Delete del = new Delete(Bytes.toBytes(rk));//获得delete对象，其中持有要删除行的rowkey
            table.delete(del);
        }
        table.close();
    }

    //TODO:删除一个数据
    @Override
    public void deleteOneData(String tableName, String rowkey,String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
            Delete del = new Delete(Bytes.toBytes(rowkey));//获得delete对象，其中持有要删除行的rowkey
            del.addColumn(Bytes.toBytes(column.split(":")[0]),Bytes.toBytes(column.split(":")[1]));
            table.delete(del);
            table.close();
    }


    //TODO:查询数据
    @Override
    public void selectData(String tableName) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        ResultScanner results = table.getScanner(scan);
        for (Result result : results) { //result对应一行数据
            Cell[] cells = result.rawCells(); //获取一行的所有cells
            for (Cell cell : cells) {
                String rowkey = Bytes.toString(CellUtil.cloneRow(cell));//
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println("rowkey:" + rowkey + "\t" + family + ":" + column
                        +"\t" + value);
            }
        }
    }

    //TODO:查询单行数据
    @Override
    public void selectOneData(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes("user"), Bytes.toBytes("name"));
        get.addColumn(Bytes.toBytes("user"), Bytes.toBytes("age"));
//        get.addFamily(Bytes.toBytes("cf1")); //如果不追加列族，则查询所有列族
        Result result = table.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("查询单行");
            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String column = Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println("row:" + row +"\t" + family + ":" + column + "\t" + value);
        }
    }

    //TODO:批量查询数据
    @Override
    public List<Result> GetData(String tableName, List<String> rowkeyList) throws IOException {
        List<Get> getList=new ArrayList<>();
        Table table = connection.getTable(TableName.valueOf(tableName));
        for(String rowkey:rowkeyList){
            getList.add(new Get(Bytes.toBytes(rowkey)));
        }
        Result[] results = table.get(getList);
        return Arrays.asList(results);
    }

    //TODO:查询单条数据
    @Override
    public Result Get(String tableName, String rowkey) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        return  table.get(new Get(Bytes.toBytes(rowkey)));

    }
}
