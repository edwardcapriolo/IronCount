package com.jointhegrid.ironcount.mapreduce;

public class User {
  public String name;
  public Integer id;

  public User(){}

  public void parse(String [] cols){
    id = Integer.parseInt(cols[0]);
    name = cols[1];
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final User other = (User) obj;
    if ((this.name == null) ? (other.name != null) : !this.name.equals(other.name)) {
      return false;
    }
    if (this.id != other.id && (this.id == null || !this.id.equals(other.id))) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = 7;
    hash = 79 * hash + (this.name != null ? this.name.hashCode() : 0);
    hash = 79 * hash + (this.id != null ? this.id.hashCode() : 0);
    return hash;
  }
  
}
