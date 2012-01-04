package com.jointhegrid.ironcount.mockingbird;

import java.util.Map;
import java.util.TreeMap;

public class CounterBatcher<Key extends Comparable<? super Key>, Column extends Comparable<? super Column>> {

  public int incCounter;
  public Map<Key, Map<Column, Long>> incMap = new TreeMap<Key, Map<Column, Long>>();

  public void increment(Key key, Column columnName, long increment) {
    if (!incMap.containsKey(key)) {
      Map<Column, Long> columnValue = new TreeMap<Column, Long>();
      columnValue.put(columnName, increment);
      incMap.put(key, columnValue);
    } else {
      Map<Column, Long> columnValue = incMap.get(key);
      if (!columnValue.containsKey(columnName)) {
        columnValue.put(columnName, increment);
      } else {
        long val = columnValue.get(columnName);
        columnValue.put(columnName, val + increment);
      }
    }
    incCounter++;
  }

  public void clear() {
    incMap.clear();
    incCounter = 0;
  }

  public static void main(String[] args) {
    CounterBatcher<String, String> c = new CounterBatcher<String, String>();
    c.increment("bob", "bla", 1);
    c.increment("bob", "bla", 3);
    c.increment("bob", "x", 2);
    System.out.println(c.incMap.get("bob").get("bla"));
    System.out.println(c.incMap.get("bob").get("x"));
  }
}
