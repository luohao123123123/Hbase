package HbaseApi.dao;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;
import java.util.List;

public interface HbaseApi_Dao {
        //TODO:创建库
      void createNS(String namespace) throws IOException;
      //TODO:判断表是否存在
     boolean isExists(String tableName) throws IOException;
     //TODO:创建表
    void createTable(String tableName,String... info) throws IOException;
    //TODO:禁用并删除表
    void deleteTable(String tableName) throws IOException;
    //TODO:插入数据
    void insertData(String tableName,String rowkey,String column,String value) throws IOException;
    //TODO:删除一行或多行数据
    void deleteData(String tableName,String... rowkey) throws IOException;
    //TODO:删除一个数据
    void deleteOneData(String tableName,String rowkey,String column) throws IOException;
    //TODO:查询数据
    void selectData(String tableName) throws IOException;
    //TODO:查询单行数据
    void selectOneData(String tableName,String rowkey) throws IOException;
    //TODO:批量查询数据
    List<Result> GetData(String tableName,List<String> rowkeyList) throws IOException;
    //TODO:查询一条数据
    Result Get(String tableName,String rowkey) throws IOException;

}
