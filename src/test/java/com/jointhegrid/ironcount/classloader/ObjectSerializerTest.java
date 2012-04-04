package com.jointhegrid.ironcount.classloader;

import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.File;
import java.io.BufferedOutputStream;
import java.io.Serializable;
import org.junit.Test;
import static org.junit.Assert.*;

public class ObjectSerializerTest {

  @Test
  public void testSerializeObject() throws Exception {
    Serializable s = "hey";
    ObjectSerializer instance = new ObjectSerializer();
    byte[] result = instance.serializeObject(s);
    Serializable s2 = instance.deserializeBytes(result);
    assertEquals(s, s2);
  }

  @Test
  public void writeHandlerToDisk() throws Exception {
    SerializedHandler h = new SerializedHandler();
    ObjectSerializer instance = new ObjectSerializer();
    byte[] result = instance.serializeObject(h);
    java.io.BufferedOutputStream bi = new BufferedOutputStream(
            new FileOutputStream(new File("/tmp/abd")));
    bi.write(result);
    bi.close();

    BufferedInputStream in = new BufferedInputStream(
            new FileInputStream(new File("/tmp/abd") ));
    
  }

}