package com.jointhegrid.ironcount.httpserver;

import java.util.Properties;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;

public class ICHTTPServer {

  public static final String IC_HTTP_DOCBASE = "ic.http.docbase";
  public Server server;
  public String docBase;

  public ICHTTPServer() {
    docBase=".";
  }

  public void startServer() throws Exception  {
    server = new Server();
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setPort(8766);
    server.addConnector(connector);
    ResourceHandler handler = new ResourceHandler();
    handler.setDirectoriesListed(true);
    //handler.setWelcomeFiles(new String [] {"index.html"});
    handler.setResourceBase(this.docBase);
    HandlerList handlers = new HandlerList();
    handlers.setHandlers( new Handler[] { handler, new DefaultHandler() });
    server.setHandler(handlers);
    server.start();
    //server.join();
  }

  public static void main (String [] args) throws Exception{
    ICHTTPServer s = new ICHTTPServer();
    Properties p = System.getProperties();
    if ( p.getProperty(IC_HTTP_DOCBASE) != null){
      s.docBase=p.getProperty(IC_HTTP_DOCBASE);
    }
    s.startServer();
  }
}
