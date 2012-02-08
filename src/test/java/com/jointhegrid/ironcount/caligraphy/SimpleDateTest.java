/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jointhegrid.ironcount.caligraphy;

import org.junit.Assert;
import java.text.ParseException;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author edward
 */
public class SimpleDateTest {

    public SimpleDateTest() {
    }

  @BeforeClass
  public static void setUpClass() throws Exception {
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
  }

    @Before
    public void setUp() {
    }

    @After
    public void tearDown() {
    }

    // TODO add test methods here.
    // The methods must be annotated with annotation @Test. For example:
  //
  @Test
  public void hello() throws Exception{
    DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    Date d = null;
    
      d = df.parse("2012-04-05");
    
    Assert.assertEquals(03, d.getMonth());
    Assert.assertEquals(05, d.getDate());
    Assert.assertEquals(0, d.getSeconds());
  }

}