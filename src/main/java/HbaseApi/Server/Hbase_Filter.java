package HbaseApi.Server;

import HbaseApi.util.HbaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

//TODO:列族过滤器
public class Hbase_Filter {
    public static void main(String[] args) {
        Connection connection = HbaseUtil.getConnection();
        try {
            Table table = connection.getTable(TableName.valueOf("Test1:test1"));
            Scan scan = new Scan();


            // todo:SubstringComparator:包含
            //todo:BinaryComparator：完全匹配

            //设置rowkey滤器
            RowFilter rowFilter=new RowFilter(CompareOperator.GREATER,new BinaryComparator("row1".getBytes()));
            //设置列族过滤器
            FamilyFilter familyFilter=new FamilyFilter(CompareOperator.EQUAL,new BinaryComparator("rowkey".getBytes()));
            //设置属性过滤器
            QualifierFilter qualifierFilter=new QualifierFilter(CompareOperator.EQUAL,new BinaryComparator("name".getBytes()));


            //装进过滤器List
            FilterList filterList=new FilterList(rowFilter,familyFilter,qualifierFilter);  //过滤rowkey是：row1，列族为：rowkey1，属性为：name的值
            scan.setFilter(filterList);

            ResultScanner results = table.getScanner(scan);
            //遍历数据
            Iterator<Result> iterator = results.iterator();
            while ((iterator.hasNext())) {
                String data = Bytes.toString(iterator.next().getValue("rowkey".getBytes(), "name".getBytes()));
                System.out.println(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Configuration config = HbaseUtil.getConfig();

    }
}
