package com.dant.webservices;

import org.junit.*;

import java.util.ArrayList;
import java.util.List;

public class UtilsTest {
	@Test
	public void testIsKeyword(){
		String a = "WHERE";
		Assert.assertEquals(true, Utils.isKeyword(a));
	}	
	@Test
	public void testIsWhereOperator(){
		String a = ">";
		Assert.assertEquals(true, Utils.isWhereOperator(a));
	}	
	@Test
	public void testIsBooleanOperator(){
		String a = "AND";
		Assert.assertEquals(true, Utils.isBooleanOperator(a));
	}	
	@Test
	public void testMatchArrays(){
		//depreced
	}

	@Test
	public void testEqualsArrays(){
		byte[] wheresEqualsPos = null;
		byte[] indexPos = null;
		Assert.assertEquals(true, Utils.equalsArrays(wheresEqualsPos,indexPos));

	}	
	@Test
	public void testIntersection(){
		List<Object> List1 = new ArrayList<>();
		List<Object> List2 = new ArrayList<>();

		Assert.assertEquals(List1, Utils.intersection(List1,List2));



	}
}
