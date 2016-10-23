package it.uniroma3.sii.homework.hbase;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class RowBean implements Serializable{

	private static final long serialVersionUID = 1L;
	private String table;
	private String rowKey;
	private Map<String, String> rowContent;

	public RowBean(){
		super();
	}

	public RowBean(String rowKey, String table, Map<String, String> rowContent) {
		super();
		this.rowKey = rowKey;
		this.table = table;
		this.rowContent = rowContent;
	}

	public RowBean(String rowKey, String table) {
		super();
		this.rowKey = rowKey;
		this.table = table;
		this.rowContent = new HashMap<String, String>();
	}

	public void addRowContent(String family, String qualifier, String value){
		String key = family + ":" + qualifier;

		this.rowContent.put(key, value);		
	}

	@Override
	public String toString() {
		return "RowBean [tableName=" + table + ", rowKey=" + rowKey + ", rowContent=" + rowContent + "]";
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public Map<String, String> getRowContent() {
		return rowContent;
	}

	public void setRowContent(Map<String, String> rowContent) {
		this.rowContent = rowContent;
	}	

}
