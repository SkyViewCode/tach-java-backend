
package tach.converter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.Map;

/**
 * This class takes the Details files created in the Converter.java and generates three files
 * which are intended as inputs to the PostgresQL copy command for the missions, notices and details tables.
 * @author tmcglynn
 */
public class DetailsToCopy {
    
    public static void main(String[] args) throws Exception {
        new DetailsToCopy().run(args[0]);
    }
    
    FileFilter  ff = new FileFilter() {
        public boolean accept(File pathname) {
            return pathname.getName().endsWith(".details");
        }        
    };
    
    DBConnect dbInfo;
    
    Map<String, Integer> missions;
    int maxMission;
    int maxNotice;
    
    String lastNotice = null;
    
    BufferedWriter mWr;
    BufferedWriter nWr;
    BufferedWriter dWr;
    
    
    void run(String dir) throws Exception {
        
        dbInfo     = new DBConnect();
        missions   = dbInfo.getMissions();
        maxMission = dbInfo.getMaxMissionID();
        maxNotice  = dbInfo.getMaxNoticeID();
        
        mWr = new BufferedWriter(new FileWriter("missions.copy"));
        nWr = new BufferedWriter(new FileWriter("notices.copy"));
        dWr = new BufferedWriter(new FileWriter("details.copy"));
        
        
        
        File dirFile = new File(dir);
        if (!dirFile.isDirectory()) {
            System.err.println("Usage: DetailsToCopy detailsDir");
        }
        File[] details = dirFile.listFiles(ff);
        for (File detail: details) {
            processFile(detail);
        }
        mWr.close();
        nWr.close();
        dWr.close();        
    }
    void processFile(File detail) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader(detail));
        String line = null;
        while ( (line = br.readLine()) != null) {
            processLine(line);
        }
    }
    
    void processLine(String line) throws Exception {
        String[] flds = line.split("\\|");
        switch (flds[0]) {
            case "notices":
                processNotice(flds);
                break;
            case "detail":
                processDetail(flds);
                break;
            default:
                System.err.println("Invalid type of field:"+line);            
        }
    }
    
    void processNotice(String[] flds) throws Exception {
        String mission = flds[1];
        String file    = flds[3];
        if (!missions.containsKey(mission)) {
            System.out.println("Adding mission:"+mission);
            maxMission += 1;
            mWr.write(maxMission+"|"+mission+"\n");
            missions.put(mission, maxMission);
        }
        int mid = missions.get(mission);
        maxNotice += 1;
        nWr.write(maxNotice+"|"+mid+"|"+file+"\n");
    }
    
    void processDetail(String[] flds) throws Exception {
        
        String line = flds[2];
        String key  = flds[3];
        String val  = "";
        if (flds.length > 4) {
            val  = flds[4];
        }
        
        // We've got three possible values that we need to create, 
        // the text value, the double value and the double array.
        
        flds = val.split("\\s+");
        boolean numeric = true;
        for (String fld: flds) {
            try {
                Double.parseDouble(fld);
            } catch (Exception e) {
                numeric= false;
                break;
            }
        }
        String rStr = "";
        String aStr = "";
        if (numeric  && !key.equals("Singleton")) {
            rStr = flds[0];
            
            if (flds.length > 1) {
                aStr = "{"+String.join(",",flds)+"}";
            }
        }
        dWr.write(maxNotice+"|"+line+"|"+key+"|"+val+"|"+rStr+"|"+aStr+"\n");        
    }
}
