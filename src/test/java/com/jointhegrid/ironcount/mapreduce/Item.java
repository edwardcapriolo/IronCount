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
package com.jointhegrid.ironcount.mapreduce;

public class Item {
  public Integer userfk;
  public String itemName;
  public Double price;
  
  public Item(){}

  public void parse(String [] cols){
    userfk = Integer.parseInt(cols[0]);
    itemName = cols[1];
    price = Double.parseDouble(cols[2]);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final Item other = (Item) obj;
    if (this.userfk != other.userfk && (this.userfk == null || !this.userfk.equals(other.userfk))) {
      return false;
    }
    if ((this.itemName == null) ? (other.itemName != null) : !this.itemName.equals(other.itemName)) {
      return false;
    }
    if (this.price != other.price && (this.price == null || !this.price.equals(other.price))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 53 * hash + (this.userfk != null ? this.userfk.hashCode() : 0);
    hash = 53 * hash + (this.itemName != null ? this.itemName.hashCode() : 0);
    hash = 53 * hash + (this.price != null ? this.price.hashCode() : 0);
    return hash;
  }

}
