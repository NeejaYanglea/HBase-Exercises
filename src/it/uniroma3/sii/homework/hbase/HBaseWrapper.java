package it.uniroma3.sii.homework.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;

public class HBaseWrapper {
	private Configuration config = HBaseConfiguration.create();

	public void addRecord(String tableName, String rowKey, String family,
			String qualifier, String value) throws Exception {
		HTable table = new HTable(config, tableName);

		Put put = new Put(rowKey.getBytes());

		put.add(family.getBytes(), qualifier.getBytes(), value.getBytes());

		table.put(put);

		System.out.println("added " + tableName + " " + rowKey + " " + family + ":" +
				qualifier + " " + value);

		table.close();

	}

	public void delRecord(String tableName, String rowKey) throws IOException {
		HTable table = new HTable(config, tableName);

		Delete del = new Delete(rowKey.getBytes());

		table.delete(del);

		System.out.println("deleted row " + tableName + " " + rowKey);
		table.close();		
	}

	public RowBean getOneRecord(String tableName, String rowKey) throws	IOException {
		HTable table = new HTable(config, tableName);
		RowBean row = new RowBean(rowKey, tableName);

		Get get = new Get(rowKey.getBytes());
		Result rs = table.get(get);

		for(KeyValue kv : rs.raw())
			row.addRowContent(new String(kv.getFamily()), new String(kv.getQualifier()), new String(kv.getValue()));

		table.close();
		return row;
	}

}
