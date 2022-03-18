import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class hbaseTest {
  HBaseUtil hBaseUtil;

  @BeforeEach
  public void init() throws IOException {
    hBaseUtil = new HBaseUtil();
  }

  //  测试一为获取Hbase中所有的数据
  @Test
  public void test1() throws IOException {
//    请在测试前把表名替换为自己所创建的表名
    ResultScanner info = hBaseUtil.getScanner("student");
    for (Result result : info) {
      while (result.advance()) {
        Cell cell = result.current();
        String row = new String(CellUtil.cloneRow(cell));
        String family = new String(CellUtil.cloneFamily(cell));
        String qualifier = new String(CellUtil.cloneQualifier(cell));
        String value = new String(CellUtil.cloneValue(cell));
        System.out.println(row + "->" + family + ":" + qualifier + "->" + value);
      }
    }
  }
  //  测试二为删除列
  @Test
  public void test2() throws IOException {
    boolean b = hBaseUtil.deleteQualifier("info", "hr", "PersonInfo", "age");
    System.out.println(b);
    test1();
  }
  //  测试三为删除行
  @Test
  public void test3() throws IOException {
    boolean b = hBaseUtil.deleteRow("info", "hr");
    test1();
  }
  //  测试四为插入数据
  @Test
  public void test4() throws IOException {
    //    单条插入
    boolean b = hBaseUtil.putRow("student", "hr", "grade", "math", "100");
    System.out.println(b);

    //    批量插入
    Put put = new Put(Bytes.toBytes("csh"));
    put.addColumn(Bytes.toBytes("grade"), Bytes.toBytes("math"), Bytes.toBytes("99"));
    put.addColumn(Bytes.toBytes("grade"), Bytes.toBytes("English"), Bytes.toBytes("100"));
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("x3"));
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("18"));

    List<Put> puts = new ArrayList<>();
    puts.add(put);
    boolean b2 = hBaseUtil.putRows("student", puts);
    System.out.println(b2);

  }

  //  测试五为删除表
  @Test
  public void test5() throws IOException {
    boolean info = hBaseUtil.deleteTable("student");
    System.out.println(info);
  }
  //  测试六为创建表与列族
  @Test
  public void test6() throws IOException {
    hBaseUtil.createTableWithMoreColumnFamilies("student","info","grade");
  }
//  测试七为删除列族
  @Test
  public void test7() throws IOException{
    boolean b = hBaseUtil.deleteColumnFamily("student", "info");
    System.out.println(b);
  }

//  测试八为带过滤器的按范围查询数据
  @Test
  public void test8() throws IOException{

//    MUST_PASS_ALL为全部匹配才行
//    MUST_PASS_ONE只需一个匹配就行
    FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//    前缀
    allFilters.addFilter(new PrefixFilter(Bytes.toBytes("abc")));
//    正则
    allFilters.addFilter(new RowFilter(CompareOperator.EQUAL,
            new RegexStringComparator(".*_(xyz|xxx)$")));
//    完全匹配
    allFilters.addFilter(new QualifierFilter(CompareOperator.EQUAL,
            new SubstringComparator("china")));
    ResultScanner results = hBaseUtil.getScannerWithFilter("tablename", "start", "stop", allFilters);


  }
  //  测试八为普通按行范围查询数据
  @Test
  public void test9() throws IOException{
    ResultScanner scanner = hBaseUtil.getScanner("student", "c", "d");
    for (Result result : scanner) {
      while (result.advance()) {
        Cell cell = result.current();
        String row = new String(CellUtil.cloneRow(cell));
        String family = new String(CellUtil.cloneFamily(cell));
        String qualifier = new String(CellUtil.cloneQualifier(cell));
        String value = new String(CellUtil.cloneValue(cell));
        System.out.println(row + "->" + family + ":" + qualifier + "->" + value);
      }
    }
  }

}
