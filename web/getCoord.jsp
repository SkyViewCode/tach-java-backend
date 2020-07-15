<%@page contentType="text/plain" pageEncoding="UTF-8"%><%
    java.io.OutputStream os = response.getOutputStream();
    String path = "https://heasarc.gsfc.nasa.gov/cgi-bin/Tools/convcoord/convcoord.pl?Output=batch&CoordVal="+
       request.getParameter("position");
    java.net.URL url = new java.net.URL(path);
    java.io.InputStream is = url.openStream();
    byte[] buf = new byte[4096];
    int len = 0;
    while ( (len = is.read(buf)) > 0) {
        os.write(buf, 0, len);
    }
%>