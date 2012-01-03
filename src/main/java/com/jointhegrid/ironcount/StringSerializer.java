/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.jointhegrid.ironcount;

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 *
 * @author edward
 */
public class StringSerializer implements ZkSerializer  {

  @Override
  public byte[] serialize(Object o) throws ZkMarshallingError {
    String s = (String) o;
    return s.getBytes();
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    return new String(bytes);
  }

}
