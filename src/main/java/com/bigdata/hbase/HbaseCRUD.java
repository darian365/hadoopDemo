package com.bigdata.hbase;

import java.io.IOException;
import java.util.Arrays;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.jasper.tagplugins.jstl.core.Out;

public class HbaseCRUD {
	private static final Log LOG = LogFactory.getLog(HbaseCRUD.class);
	private HBaseAdmin admin;
	private Configuration conf;
	
	private void init() throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		System.out.println("---------setUp----------");
		conf = HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);

	}
	
	private void cleanUp() throws IOException {
		System.out.println("---------CleanUp----------");
		if(admin != null) {
			admin.close();
		}
	}
	
	private void testListTable() throws Exception{
		System.out.println("--------testListTable--------");
		String[] tables = admin.getTableNames();
		System.out.println(Arrays.toString(tables));
		
		TableName[] tbs = admin.listTableNames();
		System.out.println(Arrays.toString(tbs));
		
		HTableDescriptor[] tbs1 = admin.listTables();
		for(HTableDescriptor tb : tbs1) {
			System.out.print(tb.getTableName()+" ");
		}
		System.out.println();
	}
	
	private void testCreateTable() throws IOException {
		System.out.println("--------testCreateTable--------");
		HTableDescriptor desc = new HTableDescriptor("test1");
		desc.addFamily(new HColumnDescriptor("cf1"));
		admin.createTable(desc);
	}
	
	private void testAddRecord(String tableName, String rowKey, String cFamily, String column, String value) throws IOException {
		System.out.println("--------testAddTable--------");
		HTable table = new HTable(conf, tableName);
		Put put = new Put(rowKey.getBytes());
		put.add(cFamily.getBytes(), column.getBytes(), value.getBytes());
		table.put(put);
		table.close();
		System.out.println("Added successfully");
	}
	
	private void testQueryRecord(String tableName, String rowKey) throws IOException {
		System.out.println("--------testQueryRecord--------");
		HTable table = new HTable(conf, tableName);
		Get get = new Get(rowKey.getBytes());
		Result result = table.get(get);
		KeyValue[] kvs = result.raw();
		for(KeyValue kv : kvs) {
			System.out.println(new String(kv.getQualifier()) + ":" + new String(kv.getValue()));
		}
	}
	
	private void testQueryAll(String tableName) throws IOException {
		System.out.println("--------testQueryAll--------");
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			System.out.print(new String(result.getRow())+"\t");
			for(Cell cell : result.rawCells()) {
				System.out.println(new String(cell.getQualifier()) + " = " + new String(cell.getValue())+"\t");
			}
		}
	}
	
	private void testQueryByCondition(String tableName) throws IOException {
		System.out.println("--------testQueryByCondition--------");
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		scan.addColumn("cf1".getBytes(), "name".getBytes());
		Filter filter = new SingleColumnValueFilter(
				"cf1".getBytes(),"name".getBytes(),CompareOp.EQUAL, Bytes.toBytes("demo1"));
		scan.setFilter(filter);
		ResultScanner scanner = table.getScanner(scan);
		for(Result result : scanner) {
			System.out.print(new String(result.getRow())+"\t");
			for(Cell cell : result.rawCells()) {
				System.out.println(new String(cell.getQualifier())+" = "+
						new String(cell.getValue()));
			}
		}
	}
	
	private void testDeleteRecord(String tableName, String rowKey) throws IOException {
		System.out.println("--------testDeleteRecord--------");
		HTable table = new HTable(conf, tableName);
		Delete delete = new Delete(rowKey.getBytes());
		table.delete(delete);
		table.close();
		System.out.println("--------Delete OK--------");
	}
	
	private void testDeleteTable(String tableName, String rowKey) throws IOException {
		System.out.println("--------testDeleteTable--------");
		if(admin.tableExists(tableName)) {
			admin.disableTable(tableName);
		}
		admin.deleteTable(tableName);
		System.out.println("--------Delete table OK--------");
	}
	
	public static void main(String[] args) throws Exception {
		HbaseCRUD crud = new HbaseCRUD();
		crud.init();
//		crud.testCreateTable();
		crud.testListTable();
		crud.testAddRecord("test1", System.currentTimeMillis()+"", "cf1", "name", "demo1");
		crud.testQueryRecord("test1", "1505756772679");
		crud.testQueryAll("test1");
		crud.testQueryByCondition("test1");
		crud.testDeleteRecord("test1", "1505757862730");
		crud.testQueryByCondition("test1");
		crud.cleanUp();
	}
}
