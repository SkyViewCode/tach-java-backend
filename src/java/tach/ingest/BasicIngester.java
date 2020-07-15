package tach.ingest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Ingest the data files into the database as individual lines.
 * @author tmcglynn
 */
public class BasicIngester {
    
    /**
     * @param args
     *      Usage: java tach.ingest.BasicIngester basedirectory [ext]
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: java tack.ingest.BasicIngester baseDirectory [exten]");
            return;           
        }
        
        String base = args[0];
        String ext  = null;
        if (args.length > 1) {
            ext = args[1];
        }
        new BasicIngester().processDirectory(base, ext);
    }
    
    FileWriter sql;
    
    
    public void processDirectory(String base, String ext) throws Exception {

        
        sql = new FileWriter("updates.sql");
        sql.write("drop table rawnotices;\n");
        sql.write("create table rawnotices(\n" +
                  "  mission text,\n"+
                  "  file    text,\n"+
                  "  count   int,\n"+
                  "  line    text);\n");
        sql.write("create index rawnoticesi1 on rawnotices(mission);\n");
        sql.write("create index rawnoticesi2 on rawnotices(mission,file);\n");
        sql.write("create index rawnoticesi3 on rawnotices(file);\n");
        
        
        System.out.println("Processing inside directory: "+base);
        File baseFile = new File(base);
        
        for (File sub: baseFile.listFiles()) {
            if (sub.isDirectory()) {
                processMission(sub, ext);                
            }            
        }
        sql.close();
    }
    
    public void processMission(File missDir, String ext) throws Exception {
        String mission = missDir.getName();
        System.out.println("  Processing mission:"+mission);
        cleanMission(mission);
        for (File fil: missDir.listFiles()) {
            if (ext != null) {
                String name = fil.getName();
                if (!name.endsWith(ext)) {
                    // Skip this file.
                    continue;
                }
            }
            processFile(mission, fil);
            sql.flush();
        }
    }
    
    void processFile(String mission, File f) throws Exception {
        int count = 0;
        BufferedReader br = new BufferedReader(new FileReader(f));
        String file = f.getName();
        if (f.getName().endsWith(".email")) {
            file = file.substring(0,file.length()-6);
        }
        
        String line;
        while ((line = br.readLine()) != null) {
            count += 1;
            // Double quotes for inclusion in DB.
            line = line.replace("'", "''");
            sql.write("insert into rawnotices values ('"+mission+"','"+file+"',"+count+",'"+line+"');\n");
        }
        br.close();
        System.out.println("   "+mission+": "+f.getName()+" "+count);        
    }   
    
    void cleanMission(String mission)  throws Exception {
        sql.write("delete from rawnotices where mission='"+mission+"';\n");        
    }
}
