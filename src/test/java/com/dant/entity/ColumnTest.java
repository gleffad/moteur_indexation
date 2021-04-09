package com.dant.entity;

import static org.junit.Assert.assertEquals;

import org.junit.*;

public class ColumnTest {
	@Test
	public void testConvertToType(){
		Column column0 = Column.builder().build();
		String type0 = "INT";
		String type1 = "STRING";
		String type2 = "LONG";
		String type3 = "FLOAT";
		final String value0 = "2147483647";
		final String value1 = "9223372036854775807";
		final String value2 = "9223372036854775807";
		final String value3 = "214748.3646";
		assertEquals(Integer.valueOf("2147483647"), column0.convertToType(type0, value0));
		assertEquals(value1, column0.convertToType(type1, value1));
		assertEquals(Long.valueOf("9223372036854775807"), column0.convertToType(type2, value2));
		assertEquals(Float.valueOf("214748.3646"), column0.convertToType(type3, value3));
	}
}
