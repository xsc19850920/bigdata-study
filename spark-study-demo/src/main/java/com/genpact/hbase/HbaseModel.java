package com.genpact.hbase;

import java.io.Serializable;

public class HbaseModel implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tags;
	private String qualifier;
	private String  family;
	private String  row;
	private String  value;
	public String getTags() {
		return tags;
	}
	public void setTags(String tags) {
		this.tags = tags;
	}
	public String getQualifier() {
		return qualifier;
	}
	public void setQualifier(String qualifier) {
		this.qualifier = qualifier;
	}
	public String getFamily() {
		return family;
	}
	public void setFamily(String family) {
		this.family = family;
	}
	public String getRow() {
		return row;
	}
	public void setRow(String row) {
		this.row = row;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	public HbaseModel() {
	}
	public HbaseModel(String tags, String qualifier, String family, String row, String value) {
		super();
		this.tags = tags;
		this.qualifier = qualifier;
		this.family = family;
		this.row = row;
		this.value = value;
	}
	@Override
	public String toString() {
		return "HbaseModel [tags=" + tags + ", qualifier=" + qualifier + ", family=" + family + ", row=" + row + ", value=" + value + "]";
	}
	
	
	
}
