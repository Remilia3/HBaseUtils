import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class HBaseUtil {
  Admin admin;

  public HBaseUtil() throws IOException {
    admin = HBaseConn.getHBaseConn().getAdmin();
  }

  /**
   * 删除表
   * @param tableName 表名称
   */
  public boolean deleteTable(String tableName) {
    try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
      if (!admin.tableExists(TableName.valueOf(tableName))) {
        return false;
      }
      admin.disableTable(TableName.valueOf(tableName));
      admin.deleteTable(TableName.valueOf(tableName));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * 插入数据
   * @param tableName 表名
   * @param rowKey 行键
   * @param cfName 列族名
   * @param qualifier 列名
   * @param data 数据
   * @return Boolean 是否成功
   */
  public boolean putRow(
      String tableName, String rowKey, String cfName, String qualifier, String data) {
    try (Table table = HBaseConn.getTable(tableName)) {
      Put put = new Put(Bytes.toBytes(rowKey));
      put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifier), Bytes.toBytes(data));
      table.put(put);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * 批量导入数据
   * @param tableName 表名
   * @param puts  批量导入构造器
   */
  public boolean putRows(String tableName, List<Put> puts) {
    try (Table table = HBaseConn.getTable(tableName)) {
      table.put(puts);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * 查询单条数据
   * @param tableName 表名
   * @param rowKey  行键
   * @return Result 结果集
   */
  public Result getRow(String tableName, String rowKey) {
    try (Table table = HBaseConn.getTable(tableName)) {
      Get get = new Get(Bytes.toBytes(rowKey));
      return table.get(get);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 带有过滤器的查询数据
   *
   * @param tableName 表名
   * @param rowKey 行键
   * @param filterList 过滤器
   * @return Result
   */
  public Result getRow(String tableName, String rowKey, FilterList filterList) {
    try (Table table = HBaseConn.getTable(tableName)) {
      Get get = new Get(Bytes.toBytes(rowKey));
      get.setFilter(filterList);
      return table.get(get);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * scan扫描数据
   * @param tableName 表名
   * @return ResultScanner
   */
  public ResultScanner getScanner(String tableName) {
    try (Table table = HBaseConn.getTable(tableName)) {
      Scan scan = new Scan();
      scan.setCaching(1000);
      return table.getScanner(scan);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * can 检索数据
   * @param tableName 表名
   * @param startKey 起始行
   * @param stopKey 结束行(不包括)
   * @return ResultScanner
   */
  public ResultScanner getScanner(String tableName, String startKey, String stopKey) {
    return getScannerWithFilter(tableName,startKey,stopKey,null);
  }

  /**
   * can 检索数据,带过滤器
   * @param tableName 表名
   * @param startKey 起始行
   * @param stopKey 结束行(不包括)
   * @param filterList 过滤器
   * @return ResultScanner
   */
  public ResultScanner getScannerWithFilter(
      String tableName, String startKey, String stopKey, FilterList filterList) {
    try (Table table = HBaseConn.getTable(tableName)) {
      Scan scan = new Scan();
      scan.setFilter(filterList);
      scan.withStartRow(Bytes.toBytes(startKey));
      scan.withStopRow(Bytes.toBytes(stopKey));
      scan.setCaching(1000);

      return table.getScanner(scan);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * 删除行
   * @param tableName 表名
   * @param rowKey 行键
   */
  public boolean deleteRow(String tableName, String rowKey) {
    try (Table table = HBaseConn.getTable(tableName)) {
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      table.delete(delete);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /** 删除列簇 */
  public boolean deleteColumnFamily(String tableName, String cfName) {
    try (HBaseAdmin admin = (HBaseAdmin) HBaseConn.getHBaseConn().getAdmin()) {
      admin.deleteColumnFamily(
          TableName.valueOf(tableName), Bytes.toBytes(cfName));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * 删除列
   * @param tableName 表名
   * @param rowKey 行键
   * @param cfName 列簇名
   * @param qualifierName 列名
   */
  public boolean deleteQualifier(
      String tableName, String rowKey, String cfName, String qualifierName) {
    try (Table table = HBaseConn.getTable(tableName)) {
      Delete delete = new Delete(Bytes.toBytes(rowKey));
      delete.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualifierName));
      table.delete(delete);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return true;
  }

  /**
   * 在指定表中创建一个或多个列族
   * @param tableName 表名
   * @param columnFamilies 列族名,可以有多个
   */

  public void createTableWithMoreColumnFamilies(String tableName, String... columnFamilies)
      throws IOException {
    // 表的构建器
    TableDescriptorBuilder tableDescriptorBuilder =
        TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));

    // 列族构建器
    List<ColumnFamilyDescriptor> cfs = new ArrayList<>();
    for (String columnFamily : columnFamilies) {
      ColumnFamilyDescriptorBuilder cf =
          ColumnFamilyDescriptorBuilder.newBuilder(columnFamily.getBytes());
      ColumnFamilyDescriptor columnFamilyDescriptor = cf.build();
      cfs.add(columnFamilyDescriptor);
    }
    // 表的构建器 添加列族
    tableDescriptorBuilder.setColumnFamilies(cfs);
    // 表描述对象
    TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
    // 创建表
    admin.createTable(tableDescriptor);
  }
}
