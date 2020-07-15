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
public class JsonQuery extends HttpServlet {

    private static Pattern hexEsc = Pattern.compile("%[0-9a-fA-F][0-9a-fA-F]");

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
    
    public static String makeQueryReturnJson(String sql) {            
        return   "select array_to_json(array_agg(row_to_json(t))) from (" +
                         sql + 
                      ") t";            
    }
    
    class Impl {
        
        HttpServletRequest  req;
        HttpServletResponse resp;
        
        /** Wrap user SQL to use the Postgres JSON capabilities
         *  to return a single JSON string as the result.
         * @param sql  Input sql.
         * @return Modified SQL that will return the results of the original query
         * as a single JSON string.
         */
        void processRequest(HttpServletRequest request, HttpServletResponse response, boolean isPost) 
                throws ServletException, IOException {
            
            String result;
            response.setContentType("text/json");
            Writer wr = response.getWriter();
            try {
                
                String[] params = request.getParameterValues("sql");
                String sql = params[0];
                System.err.println("Checking sql:\n"+sql);
                heasarc.parsetest.SecurityChecker chkr = new heasarc.parsetest.SecurityChecker();
                chkr.check(sql);            
                sql = makeQueryReturnJson(sql);
                
                System.err.println("Looking at query:"+sql);
                
                Connection conn = new tach.converter.DBConnect().getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs   = stmt.executeQuery(sql);
                if (!rs.next()) {
                    throw new Exception("No results");
                }
                result = "{\"status\": true, \"results\":"+rs.getString(1) + "}";
                System.err.println("Got result");
                rs.close();
                
                
            } catch (Exception e) {
                System.err.println("Got exception:"+e);
                System.err.println("    Reason:"+e.getMessage());
                e.printStackTrace(System.err);
                
                String msg = e.getMessage();
                msg = msg.replace("\n", " ");
                msg = msg.replace("\\\\", "\\\\\\\\");
                msg = msg.replace("\"", "\\\"");
                result = "{\"status\": false, \"reason\":\""+ msg + "\"}";
            }
            wr.write(result);
            System.err.println("Writing result");
        }
    }    
}
