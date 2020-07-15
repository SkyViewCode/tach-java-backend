/*
 * Developed by T. McGlynn as part of the TACH project.
 * 
 */
package tach.ingest;

import java.sql.Statement;
import tach.converter.DBConnect;

/**
 *
 * @author tmcglynn
 */
public class DBCleaner {
    
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            args = new String[]{"-1", "-1"};
        } else if (args.length == 1) {
            args = new String[]{args[0], "-1"};
        }
        int limitNotice     = Integer.parseInt(args[0]);
        int limitInstrument = Integer.parseInt(args[1]);
        
        DBConnect conn = new DBConnect();
        int maxInstrument = conn.getMaxMissionID();
        int maxNotice     = conn.getMaxNoticeID();
        System.out.println("At start:");
        System.out.println("    Max mission/instrument:  "+maxInstrument);
        System.out.println("    Max notice:              "+maxNotice);
        
        Statement stmt = conn.getConnection().createStatement();
        if (limitInstrument > 0) {
            stmt.execute("delete from missions where mid > "+maxInstrument);
        }
        
        if (limitNotice > 0) {
            stmt.execute("delete from notices where nid > "+maxNotice);
            stmt.execute("delete from details where mid > "+maxNotice);
        }
        stmt.close();
        conn.getConnection().close();
    }
    
}
