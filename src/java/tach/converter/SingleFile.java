/*
 * Developed by T. McGlynn as part of the TACH project.
 * 
 */
package tach.converter;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 *
 * @author tmcglynn
 */
public class SingleFile  extends Converter {
    
    public static void main(String[] args) {
        try {
            SingleFile sf = new SingleFile();
            sf.process();
        } catch (Exception e) {
            System.out.println("Error processing file:"+e);
            e.printStackTrace(System.out);
        }
    }
    
    String mission;
    int lineCount = 0;
    int noticeNumber;
    int missionID;
    
    void process() throws Exception {
        BufferedReader rdr = new BufferedReader(new InputStreamReader(System.in));
        String line;
        
        while ( (line = rdr.readLine()) != null) {
            convertLine(line);
            lineCount += 1;
        }
        
    }
    
    void handleNoticeType(String type) {
        mission = getMission(type);
        System.out.println("Got mission:"+mission+" from type "+type);
    }
    
    String getMission(String type) {
        noticeNumber = getMaxNotice() + 1;
        type = type.toLowerCase();
        String mission = null;
        String[] flds = type.split("( |-)");
        if (flds[0].equals("coincidence")) {
            mission =  "coinc";
        } else if (flds[0].equals("real") ||
             (flds[0].equals("test") && ! flds[1].equals("integral")) ) {
            mission = "snews";
        } else if (flds[0].equals("test") && flds[1].equals("integral")) {
            mission = "integral";
        } 
        missionID = getMissionID(mission);        
        return mission;                
    }
    
    int getMissionID(String mission) {
        return 1;
    }
    
    void handleNoticeDate(String date) {
        date = date.replace("\\", "\\\\");
        date = date.replace("'", "\\'");
        executeSQL("insert into notices values("+missionID+","+noticeNumber+","+date);
    }
    
    int getMaxNotice() {
        return 1000;
    }
    void executeSQL(String sql) {
        System.out.println("SQL:"+sql);
    }
    
    SingleFile() throws Exception {
        super("dynamic");
    }
    
    void emit(String key, String value) {
        String sql = "insert into details values(";
        key   = key.replace("\\", "\\\\");
        key   = key.replace("'", "\\'");
        value = value.replace("\\", "\\\\");
        value = value.replace("'", "\\'");
        
        String[] flds = value.split("\\s+");
        boolean numeric = true;
        for (String fld: flds) {
            try {
                Double.parseDouble(fld);
            } catch (Exception e) {
                numeric= false;
                break;
            }
        }
        String rStr = "null";
        String aStr = "null";
        if (numeric  && !key.equals("Singleton")) {
            rStr = flds[0];
            
            if (flds.length > 1) {
                aStr = "{"+String.join(",",flds)+"}";
            }
        }
        
        sql += noticeNumber+","+lineCount+",'"+key+"','"+value+"',"+rStr+","+aStr+")";
        executeSQL(sql);
    }    
}
