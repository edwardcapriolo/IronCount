package com.jointhegrid.ironcount.classloader;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;

public class ObjectSerializer {
  public ObjectSerializer(){

  }
  public byte [] serializeObject(java.io.Serializable s) throws IOException{
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    java.io.ObjectOutputStream os = new java.io.ObjectOutputStream
            (new BufferedOutputStream(ba));
    os.writeObject(s);
    os.flush();
    os.close();
    return ba.toByteArray();
  }

  public Serializable deserializeBytes(byte [] b) throws IOException, ClassNotFoundException {
    java.io.ObjectInputStream is = new ObjectInputStream( new ByteArrayInputStream(b));
    return (Serializable) is.readObject();
  }
}
