/*
Copyright 2011 Edward Capriolo

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
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
