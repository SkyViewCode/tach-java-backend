package tach.converter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author tmcglynn
 */
public class DBConnect {
    
    final static String dbURL = "jdbc:postgresql://dbms2.gsfc.nasa.gov:5432/tach2";
//    final static String user  = "postgres"; 
//    final static String pw    = "Ngc7424_GS";
    final static String user  = "tach_dba"; 
    final static String pw    = "Grb_190114C";
//     final static String user  = "webuser"; 
//    final static String pw    = "webpwd";
    
    
    
    private Connection conn;
    private int maxMissionID;
    private int maxNoticeID;
    private int maxDetailID;
    private Map<String,Integer> missions;
    
    public Connection getConnection()  throws Exception {
        if (conn == null) {
            Class.forName("org.postgresql.Driver");
            conn = DriverManager.getConnection(dbURL, user, pw);
        }
        return conn;        
    }
        
    public int getMission(String miss) {
        if (missions.containsKey(miss)) {
            return missions.get(miss);
        } else {
            return -1;
        }
    }
    
    public Map<String, Integer> getMissions() throws Exception {
        if (missions != null) {
            return missions;
        }
        
        Map<String,Integer> missions = new HashMap<String,Integer>();
        String sql = "select name,mid from missions";
        if (conn == null) {
            conn = getConnection();
        }
        Statement stmt = conn.createStatement();
        ResultSet rs   = stmt.executeQuery(sql);
        while (rs.next()) {
            String name = rs.getString(1);
            int    mid  = rs.getInt(2);
            if (missions.containsKey(name)) {
                System.err.println("Ambiguous misson name:"+name+" -> "+missions.get(name)+" and "+mid);
            }
            missions.put(name,mid);
        }
        rs.close();
        stmt.close();
        return missions;
         
    }
    
    public static void main(String[] args) throws Exception {
        new DBConnect();
    }
    
    public DBConnect() throws Exception {
        missions = getMissions();
        getIdMaxes();
    } 
    
    
    public int addMission(String mission) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("insert into missions(mid,name) values(?,?)");
        stmt.setInt(1, maxMissionID+1);
        stmt.setString(2, mission);
        if (checkSingleUpdate(stmt)) {
            maxMissionID += 1;
            System.err.println("Adding new mission: "+mission+" "+maxMissionID);
            return maxMissionID;
        } else {
            return -1;
        }
    }
    boolean checkSingleUpdate(PreparedStatement stmt) throws Exception {
        boolean hasRS = stmt.execute();
        boolean status = true;
        if (hasRS) {
            System.err.println("Update returns unexpected result set");
            status = false;
        } else if (stmt.getUpdateCount() != 1) {
            System.err.println("Update count is not 1: "+stmt.getUpdateCount());
            status = false;
        }
        stmt.close();
        return status;
    }
    
    public int addNotice(int mission, String noteName) throws Exception {
        
        PreparedStatement stmt = conn.prepareStatement("insert into notices(nid, mid, file) values (?,?,?)");
        stmt.setInt(1, maxNoticeID+1);
        stmt.setInt(2, mission);
        stmt.setString(3, noteName);
        if (checkSingleUpdate(stmt)) {
            maxNoticeID += 1;
            System.err.println("Added notice: "+mission+" "+maxNoticeID+" "+noteName);
            return maxNoticeID;
        } else {
            return -1;
        }            
    }
    
    void getIdMaxes() throws Exception {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("select max(mid) from missions");
        rs.next();
        maxMissionID = rs.getInt(1);
        if (rs.wasNull()) {
            maxMissionID = 0;
        }
        rs.close();
        rs = stmt.executeQuery("select max(nid) from notices");
        rs.next();
        maxNoticeID = rs.getInt(1);
        if (rs.wasNull()) {
            maxNoticeID = 0;
        }
        rs.close();
        rs = stmt.executeQuery("select max(nid) from notices");
        rs.next();
        maxNoticeID = rs.getInt(1);
        if (rs.wasNull()) {
            maxNoticeID = 0;
        }
    }
    
    public int getMaxMissionID() {
        return maxMissionID;
    }
    
    public int getMaxNoticeID() {
        return maxNoticeID;
    }
}
