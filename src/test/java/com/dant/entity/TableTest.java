package com.dant.entity;

import java.util.*;

import org.junit.*;

public class TableTest {
	private Table table;

	@Test
	public void testGetColumnNames() {
		ArrayList<String> colNames = new ArrayList<>(); colNames.add("COLUMN1");
		colNames.add("COLUMN2");
		String tableName = "testtable0";
		String[] columnNames = new String[] { "column1", "column2" };
		Column[] columns = new Column[] { Column.builder().name(columnNames[0]).type("ANY").build(),
				Column.builder().name(columnNames[1]).type("ANY").build() };
		table = Table.builder().name(tableName).columns(columns).build();
		final List<String> should_return = colNames;
		final List<String> returned = table.getColumnNames();
		Assert.assertEquals(should_return, returned);
	}

	@Test
	public void testGetName() {
		String tableName = "testtable0";
		table = Table.builder().name(tableName).build();
		final String should_return = "TESTTABLE0";
		final String returned = table.getName();
		Assert.assertEquals(should_return, returned);
	}

	@Test
	public void testInsert() {
	
	}

	@Test
	public void testGetAtPosition() {

	}

	@Test
	public void testGetNext() {

	}

	@Test
	public void testGetAll() {

	}
}
