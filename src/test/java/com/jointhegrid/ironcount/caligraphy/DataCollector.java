package com.jointhegrid.ironcount.caligraphy;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public class DataCollector {

  FSDataOutputStream s;
  int linecount;
  int max_lines=1;
  CaligraphyMessageHandler parent;
  Path base;

  public DataCollector(CaligraphyMessageHandler parent){
    this.parent=parent;
    base = new Path("/tmp/caligraphy");
  }

  void write(Date d, String[] message) {
    try {
      Path dateDir = new Path(base, parent.df.format(d));
      if (!parent.fs.exists(dateDir)) {
        parent.fs.mkdirs(dateDir);
      }
      if (s == null) {
        newOut(dateDir);
      }
      if (this.linecount >= this.max_lines) {
        newOut(dateDir);
        this.linecount = 0;
      }
      this.s.writeUTF(StringUtils.join(message, " "));
      this.s.sync();
    } catch (IOException ex) {
      Logger.getLogger(DataCollector.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private void newOut(Path dateDir) {
    String name = UUID.randomUUID().toString();
    Path p = new Path(dateDir, name);
    try {
      s = parent.fs.create(p);
    } catch (IOException ex) {
      Logger.getLogger(DataCollector.class.getName()).log(Level.SEVERE, null, ex);
    }
  }
}