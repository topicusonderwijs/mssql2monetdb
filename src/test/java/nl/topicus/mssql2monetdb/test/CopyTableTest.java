package nl.topicus.mssql2monetdb.test;

import nl.topicus.mssql2monetdb.CopyTable;

import org.junit.Assert;
import org.junit.Test;

public class CopyTableTest {

	@Test
	public void testGenerateCountQuery() {
		CopyTable table = new CopyTable();
		
		// test standard count query based on table from name
		table.setFromName("test");
		Assert.assertEquals(table.generateCountQuery(), "SELECT COUNT(*) FROM [test]");
		
		table.setFromName(null);
		
		// test a custom count query
		table.setFromCountQuery("SELECT COUNT(*) FROM myQuery WHERE bla = 'something'");		
		Assert.assertEquals(table.generateCountQuery(), "SELECT COUNT(*) FROM myQuery WHERE bla = 'something'");	
	}
	
	@Test
	public void testGenerateSelectQuery () {
		CopyTable table = new CopyTable();
		
		// test standard select all query
		table.setFromName("test");
		Assert.assertEquals(table.generateSelectQuery(), "SELECT * FROM [test]");
		
		// test custom columns select query
		table.setFromColumns("col1, col2");
		Assert.assertEquals(table.generateSelectQuery(), "SELECT col1, col2 FROM [test]");
		
		table.setFromName(null);
		table.setFromColumns(null);
		
		// test custom select query
		table.setFromQuery("SELECT col1, col2 FROM myQuery WHERE bla = 'something'");
		Assert.assertEquals(table.generateSelectQuery(), "SELECT col1, col2 FROM myQuery WHERE bla = 'something'");
	}

}