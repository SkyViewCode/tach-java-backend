/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tach.converter;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author tmcglynn
 */
public class Utils {
    
    public static Map<String, Integer> monthMap;
    public static String[][] monthStrings = {
        {"jan", "january"}, {"feb", "february"}, {"mar", "march"},
        {"apr", "april"},   {"may"},             {"jun", "june"},
        {"jul", "july"},    {"aug", "august"},   {"sep", "september"},
        {"oct", "october"}, {"nov", "november"}, {"dec", "december"}            
    };
    
    static java.util.Calendar utCal = new java.util.GregorianCalendar(java.util.TimeZone.getTimeZone("GMT"));
    
    
    // Initialize the monthMap.
    static {
        monthMap = new HashMap<String, Integer>();
        int index = 1;
        for (String[] months: monthStrings) {
            for (String mon: months) {
                monthMap.put(mon, index);
            }
            index += 1;
        }
    }
    
    private static class Clock {
        int hr;
        int mn;
        double sc;        
    }
    
    /** Convert ISO Date to MJD */
    public static double mjd(int year, int mon, int day) {
        return mjd(year,mon,day,0,0,0);
    }
    
    /** Convert ISO Date to MJD */
    public static double mjd(int year, String mon, int day) {
        return mjd(year, month(mon), day, 0, 0, 0);                
    }
    
    /** Convert ISO Date to MJD */
    public static double mjd(int year, String mon, int day, String tod) {
        Clock c = getClock(tod);
        return mjd(year, month(mon), day, c.hr, c.mn, c.sc);                
    }
    
    /** Convert ISO Date to MJD */
    public static double mjd(int year, int mon, int day, int hr, int mn, double sc) {
        if (mon < 1 || mon > 12) {
            return Double.NaN;
        }
        if (year < 50) {
            year += 2000;
        }
        if (year < 100 && year > 79) {
            year += 1900;
        }
        
        utCal.set(year, mon - 1, day, hr, mn, (int) sc);

        // Calendar is really stupid so I need to do this separately...
        utCal.set(java.util.Calendar.MILLISECOND, (int) ((sc - (int) sc) * 1000));

        java.util.Date epoch = utCal.getTime();

        // We are ignoring leap seconds
        double dt = epoch.getTime() / 86400000. + 40587;
        return dt;
    } 

    /** Convert a String month to a 1-12 integer. */    
    public static int month(String input) {
        input = input.toLowerCase();
        if (monthMap.containsKey(input)) {
            return monthMap.get(input);
        } else {
            return 0;
        }        
    }
    
    public static Clock getClock(String input) {
        String[] flds = input.split(":");
        if (flds.length == 1 || flds.length > 3) {
            return null;
        } else {
            int hr = Integer.parseInt(flds[0]);            
            int mn = -1;
            double sc = -1;
            if (flds.length == 3) {
                mn = Integer.parseInt(flds[1]);
                sc = Double.parseDouble(flds[2]);                
            } else {
                double xmn = Double.parseDouble(flds[1]);
                mn = (int) Math.floor(xmn);
                sc = (xmn-mn)*60;
            }
            Clock c = new Clock();
            c.hr = hr;
            c.mn = mn;
            c.sc = sc;
            return c;
        }
    }    
}
