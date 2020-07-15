/*
 * Developed by T. McGlynn as part of the TACH project.
 *
 */
package tach.ingest;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.text.SimpleDateFormat;
import tach.converter.DBConnect;
import tach.converter.LineProcessor;

/**
 *
 * @author tmcglynn
 */
public class MailIngester {

    DBConnect     init;
    int           mid;   // Mission identifier
    int           nid;   // Notice identifier
    int           evtCount;
    static Date   start;
    LineProcessor lp;
    
    public static void main(String[] args) {
        start = new Date(); 
       
            try {        
                new MailIngester().processInput();
            } catch (Exception e) {
                System.err.println("Exception found:"+e);
                e.printStackTrace(System.err);
            }
    }

    public MailIngester() throws Exception {
        init = new DBConnect();
        lp   = new LineProcessor();
    }

    public void processInput() throws Exception {
        List<String> input = new ArrayList<String>();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ( (line=br.readLine()) != null) {
            input.add(line);
        }
        System.in.close();
        processList(input);
    }

    void processList( List<String>input) throws Exception {

        int state = 0; // Looking for subject
        for (String line: input) {
            switch (state) {
                case 0:
                    if (line.matches("^Subject:.*$")) {
                        nid = getNotice(line);
                        if (nid < 0) {
                            System.err.println("Unable to find mission/notice in subject:"+line);
                            return;
                        }
                        lp.initialize(nid, init.getConnection());
                        state = 1; // Looking for blank line;
                    }

                    break;
                case 1:
                    if (line.length() == 0) {
                        state = 2; // Start of actual notice content
                    }
                    break;
                case 2:
                    processLine(line);
                    break;
                default:
                    System.err.println("Unexpected state: "+state+" in analysis");
            }
        }
	updateMaster();	
        lp.closeStmt();
	summarize();
    }
							
    void summarize() {
	System.err.println("Processing notice #"+nid);
	System.err.println("  Started:"+start);
	System.err.println("  Number of details: "+lp.linesWritten());
	System.err.println("  Number of time/position entries: "+evtCount);
	System.err.println("  Finished: "+new Date());
    }
					       

    void processLine(String line) {
         lp.processLine(line, nid);
    }

    /** Get the notice ID for this new notice. */
    int getNotice(String line) throws Exception {
        String[] fields = line.split(" ");
        String miss;
        if (fields[1].startsWith("BACODINE")) {
            miss = "bacodine";
        } else if (fields[1].startsWith("GCN/")) {
            miss = fields[1].substring(4);
            int pos = miss.indexOf("_");
            if (pos > 0) {
               miss = miss.substring(0, pos);
            }
            miss = miss.toLowerCase();
            if (miss.equals("coincidence")) {
                miss = "coinc";
            }
        } else {
            return -1;
        }
        mid = init.getMission(miss);

        if (mid == -1) {
            mid = init.addMission(miss);
        }

        if (mid >= 0) {
            Date now = new Date();
            SimpleDateFormat df = new SimpleDateFormat("YYYYMMdd-HHmmss.SSS");
            String fname = miss+"."+df.format(now);
            return init.addNotice(mid, fname);
        }
        return mid;
    }
    
    void updateMaster() {
        try {
	    
/* The following SQL is trying to add all possible combinations of time and
 * position to the master table.
 *    The first select finds all of the date/time combinations in the current notice.
 *    We add the mission id and notice id into the output of this select.
 *    The second select (in the join clause) finds all of the ra/dec combinations.
 *    We do a full outer join (in case time or position is missing) and add the results
 *    to the master table.
 */
	    
	   String sql =       
"insert into master (mid,nid,date_key,datetime,ra_key,ra,dec) "+
" (select ? as mid, ? as nid, x.date_key as date_key, x.datetime as datetime ,y.ra_key as ra_key,"+	      
"  y.ra as ra,y.dec as dec from " + 
"   (select c.date_key, a.realval+b.realval as datetime from details a, details b, timekeys c "+
"       where a.nid=? and b.nid=a.nid "+ 
"        and"+
"     a.key = c.date_key and b.key = c.time_key"+
") as x"+ 

" full outer join "+
" (select c.ra_key, a.realval as ra,b.realval as dec from details a, details b, poskeys c"+
"     where a.nid=? and b.nid=a.nid "+ 
"        and  "+
"     a.key = c.ra_key and b.key = c.dec_key "+
") as y "+
" on 1=1)";	    
	    java.sql.PreparedStatement pstmt = init.getConnection().prepareStatement(sql);
	    pstmt.setInt(1, mid);
	    pstmt.setInt(2, nid);
	    pstmt.setInt(3, nid);
	    pstmt.setInt(4, nid);
	    
	    evtCount = pstmt.executeUpdate();
	} catch (Exception e) {
	    System.err.println("Error updating master table for notice: "+nid);
	    System.err.println("   Reason: "+e.getMessage());
	}	      
    }
	
}
