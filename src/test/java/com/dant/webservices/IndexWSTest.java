package com.dant.webservices;

import com.dant.entity.*;
import org.junit.*;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class IndexWSTest {
	private IndexWS indexWS;

	@Test
	public void testCreateIndex() throws Exception {
		final String tableName = "testTableName";
		final String[] columnNames = new String[] { "column1", "column2" };
		final Table table = Table.builder().name("TESTTABLENAME").columns(
				new Column[] { Column.builder().name("column1").build(), Column.builder().name("column2").build() })
				.build();
		final Response shouldReturn = Response.ok(table).build();
		indexWS = IndexWS.getInstance();
		TableWS.getInstance().getTables().add(table);
		final Response returned = indexWS.createIndex(tableName, "BPTree_Semi_Disk",false, columnNames);
		Assert.assertEquals(shouldReturn.getEntity(), returned.getEntity());
	}

	@Test
	public void testGetAllIndexFromTableName() throws Exception {
		final String tableName = "testTableName";
		final String[] columnNames = new String[] { "column1", "column2" };
		final Table table = Table.builder().name("TESTTABLENAME").columns(
				new Column[] { Column.builder().name("column1").build(), Column.builder().name("column2").build() })
				.build();
		indexWS = IndexWS.getInstance();
		TableWS.getInstance().getTables().add(table);
		indexWS.createIndex(tableName, "BPTree_Semi_Disk",false, columnNames);
		final ArrayList<Index> should_return = new ArrayList<Index>();
		should_return.add(table.getIndex(columnNames));
		table.setIndexes(should_return);
		final List<Index> returned = indexWS.getAllIndexFromTableName(tableName);
		Assert.assertEquals(should_return, returned);
	}

	@Test
	public void testGetOneIndexFromTableNameAndColumnNames() throws Exception {
		final String tableName = "testTableName";
		final String[] columnNames = new String[] { "column1", "column2" };
		final Table table = Table.builder().name("TESTTABLENAME").columns(
				new Column[] { Column.builder().name("column1").build(), Column.builder().name("column2").build() })
				.build();
		indexWS = IndexWS.getInstance();
		TableWS.getInstance().getTables().add(table);
		indexWS.createIndex(tableName,"BPTree_Semi_Disk",false, columnNames);
		final ArrayList<Index> indexes = new ArrayList<Index>();
		indexes.add(table.getIndex(columnNames));
		table.setIndexes(indexes);
		final Index should_return = table.getIndex(columnNames);
		final Index returned = indexWS.getOneIndexFromTableNameAndColumnNames(tableName, columnNames);
		Assert.assertEquals(should_return, returned);
	}
}
