/*
 * Developed by T. McGlynn as part of the TACH projection.
 * 
 */
package tach.actions;

import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.regex.Pattern;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This servet executes a simple query againts the database
 * @author tmcglynn
 */
public class BaseQuery extends HttpServlet {

    private static Pattern hexEsc = Pattern.compile("%[0-9a-fA-F][0-9a-fA-F]");

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        new Impl().processRequest(request, response, false);
    }

    /**
     * Handles the HTTP <code>POST</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        new Impl().processRequest(request, response, true);
    }
    
    class Impl {
        
        HttpServletRequest  req;
        HttpServletResponse resp;
        void processRequest(HttpServletRequest request, HttpServletResponse response, boolean isPost) 
                throws ServletException, IOException {
            String[] params = request.getParameterValues("sql");
            String sql = params[0];
            response.setContentType("text/html");
            Writer wr = response.getWriter();
            wr.write("<h1> Base Query Response</h1>\n");
            wr.write("Input sql is:"+sql+"<br>\n"); 
            wr.write("Syntax and security check<br>\n");
            wr.flush();
            heasarc.parsetest.SecurityChecker chkr = new heasarc.parsetest.SecurityChecker();
            chkr.check(sql);
            wr.write("Security test passed<br>Executing query<br>\n");
            wr.write("Results<br>\n");
            try {
                Connection conn = new tach.converter.DBConnect().getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs   = stmt.executeQuery(sql);
                ResultSetMetaData rsmd = rs.getMetaData();
                int ncol = rsmd.getColumnCount();
                int nrow = 0;
                wr.write("<br><table border=1>\n<tr>\n");
                for (int i=1; i<=ncol; i += 1) {
                    wr.write("<th>"+rsmd.getColumnName(i)+"</th>");
                }
                wr.write("</tr>");
                while (rs.next()) {
                    wr.write("<tr>");
                    nrow += 1;
                    for (int i=1; i<=ncol; i += 1) {
                        wr.write("<td>"+rs.getObject(i)+"</td>");
                    }
                    wr.write("</tr>\n");
                }
                rs.close();
                wr.write("</table>\n<br>");
                wr.write("<br>\nNumber of columns: "+ncol+"<br>\n");
                wr.write("<br>\nNumber of rows:    "+nrow+"<br>\n");
            } catch (Exception e) {
                e.printStackTrace(System.err);
                throw new IOException("DBError",e);
            }
        }
    }    
}
