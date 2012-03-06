package com.jointhegrid.ironcount.classloader;

import com.jointhegrid.ironcount.Workload;
import java.net.MalformedURLException;
import java.net.URL;

public class ICURLClassLoader {
  public java.lang.ClassLoader getClassLoader(Workload w)
          throws MalformedURLException, ClassNotFoundException,
          InstantiationException, IllegalAccessException {
    java.net.URLClassLoader loader = null;

    if (w.classloaderUrls!=null){
      if (w.classloaderUrls.size()>0){
        loader = new java.net.URLClassLoader(
            w.classloaderUrls.toArray( new URL[]{} ));
        return loader;
      }
    }
    return java.lang.ClassLoader.getSystemClassLoader();
  }
}
