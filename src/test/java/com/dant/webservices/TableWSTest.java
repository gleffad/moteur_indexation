package com.dant.webservices;

import java.util.ArrayList;
import javax.ws.rs.core.Response;
import com.dant.entity.*;
import org.junit.*;

public class TableWSTest {
	private TableWS tableWS;

	@Test
	public void testCreateTableWithColumnNames() {
		String tableName = "testtable0";
		String[] columnNames = new String[] { "column1", "column2" };
		Column[] columns = new Column[] { Column.builder().name(columnNames[0]).type("ANY").build(),
				Column.builder().name(columnNames[1]).type("ANY").build() };
		final Table table = Table.builder().name(tableName).columns(columns).build();
		final Response should_return = Response.ok(table).build();
		tableWS = TableWS.getInstance();
		final Response returned = tableWS.createTableWithColumnNames(tableName, false, columnNames);
		Assert.assertEquals(should_return.getEntity(), returned.getEntity());
	}

	@Test
	public void testCreateTableFromNameAndColumns() {
		String tableName = "testtable0";
		String[] columnNames = new String[] { "column1", "column2" };
		Column[] columns = new Column[] { Column.builder().name(columnNames[0]).type("ANY").build(),
				Column.builder().name(columnNames[1]).type("ANY").build() };
		final Table table = Table.builder().name(tableName).columns(columns).build();
		final Response should_return = Response.ok(table).build();
		tableWS = TableWS.getInstance();
		final Response returned = tableWS.createTable(tableName, false, columns);
		Assert.assertEquals(should_return.getEntity(), returned.getEntity());
	}

	@Test
	public void testCreateTableFromNameAndJsonColumns() {

	}

	@Test
	public void testGetAllTables() {
		String tableName = "testtable0";
		String[] columnNames = new String[] { "column1", "column2" };
		Column[] columns = new Column[] { Column.builder().name(columnNames[0]).type("ANY").build(),
				Column.builder().name(columnNames[1]).type("ANY").build() };
		final Table tableTemplate = Table.builder().name(tableName).columns(columns).build();
		tableWS = TableWS.getInstance();
		tableWS.createTable(tableTemplate, false);
		ArrayList<Table> tabs = new ArrayList<Table>();
		tabs.add(tableTemplate);
		final Response should_return = Response.ok(tabs).build();
		final Response returned = Response.ok(tableWS.getAllTables()).build();
		Assert.assertEquals(should_return.getEntity(), returned.getEntity());
	}

	@Test
	public void testGetOneTable() {
		String tableName = "testtable0";
		String[] columnNames = new String[] { "column1", "column2" };
		Column[] columns = new Column[] { Column.builder().name(columnNames[0]).type("ANY").build(),
				Column.builder().name(columnNames[1]).type("ANY").build() };
		final Table tableTemplate = Table.builder().name(tableName).columns(columns).build();
		tableWS = TableWS.getInstance();
		tableWS.createTable(tableTemplate, false);
		final Response should_return = Response.ok(tableTemplate).build();
		final Response returned = Response.ok(tableWS.getOneTable(tableName)).build();
		Assert.assertEquals(should_return.getEntity(), returned.getEntity());
	}

	@Test
	public void testGetOneTableInformations() {
		String tableName = "testtable0";
		String[] columnNames = new String[] { "column1", "column2" };
		Column[] columns = new Column[] { Column.builder().name(columnNames[0]).type("ANY").build(),
				Column.builder().name(columnNames[1]).type("ANY").build() };
		final Table tableTemplate = Table.builder().name(tableName).columns(columns).build();
		tableWS = TableWS.getInstance();
		tableWS.createTable(tableTemplate, false);
		final Response should_return = Response.ok(tableTemplate).build();
		final Response returned = Response.ok(tableWS.getOneTableInformations(tableName)).build();
		Assert.assertEquals(should_return.getEntity(), returned.getEntity());
	}

	@Test
	public void testIsAvailable() {
		String tableName = "testtable0";
		String[] columnNames = new String[] { "column1", "column2" };
		Column[] columns = new Column[] { Column.builder().name(columnNames[0]).type("ANY").build(),
				Column.builder().name(columnNames[1]).type("ANY").build() };
		final Table tableTemplate = Table.builder().name(tableName).columns(columns).build();
		tableWS = TableWS.getInstance();
		tableWS.createTable(tableTemplate, false);
		final Response should_return = Response.ok(true).build();
		final Response returned = tableWS.isAvailable(tableName);
		Assert.assertEquals(should_return.getEntity(), returned.getEntity());
	}
}
