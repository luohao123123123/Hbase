package HbaseApi.Server;

import HbaseApi.daoImp.HbaseApi_DaoImp;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class HbaseApi_Server {
    public static void main(String[] args) throws IOException {
        HbaseApi_DaoImp imp = new HbaseApi_DaoImp();
        //TODO:创建库
//        imp.createNS("HbaseApi_nameSpace");
//        System.out.println("创建成功！");

        //TODO:判断表是否存在
//        boolean exists = imp.isExists("Test1:test1");

        //TODO:创建表
//        String tableName="HbaseApi_nameSpace:table_01";
//        if(imp.isExists(tableName)){
//            System.out.println("此表已经存在");
//        }
//        else {
//            imp.createTable(tableName, "user");
//            System.out.println("创建表:---" + tableName + "---成功");
//        }

        //TODO:禁用并删除表
//        imp.deleteTable("HbaseApi_nameSpace:table_01");
//        System.out.println("删除表成功");

        //TODO:插入数据
//        imp.insertData("HbaseApi_nameSpace:table_01","row2","user:age","20");
//        System.out.println("插入成功");

        //TODO:删除一行或多行数据
//        imp.deleteData("HbaseApi_nameSpace:table_01","row2");
//        System.out.println("删除成功");

        //TODO:删除一个数据
//        imp.deleteOneData("HbaseApi_nameSpace:table_01","row2","user:age");
//        System.out.println("删除成功");

        //TODO:查询所有数据
//        imp.selectData("HbaseApi_nameSpace:table_01");

        //TODO:查询单行数据
//        imp.selectOneData("HbaseApi_nameSpace:table_01","row1");
        //TODO:批量查询数据
//        List<String> rowkeyList=new ArrayList<>();
//        rowkeyList.add("row1");
//        rowkeyList.add("row2");
//         List<Result> resultList = imp.GetData("Test1:test1",rowkeyList);
//        for (Result result : resultList) {
//            System.out.println(Bytes.toString(result.getRow()));
//        }

        //TODO:查询单条数据
//        Result result = imp.Get("Test1:test1", "row1");
//        String rowkey=Bytes.toString(result.getRow());
//        String value=Bytes.toString(result.getValue(Bytes.toBytes("rowkey"),Bytes.toBytes("name")));
//        System.out.println("rowkey:"+rowkey);
//        System.out.println("value"+value);
    }
}
