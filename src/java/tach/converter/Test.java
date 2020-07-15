/*
 * Developed by T. McGlynn as part of the TACH project.
 * 
 */
package tach.converter;

import java.text.SimpleDateFormat;

/**
 *
 * @author tmcglynn
 */
public class Test {
    public static void main(String[] args) {
        java.util.Date now = new java.util.Date();
        SimpleDateFormat df = new SimpleDateFormat("YYYYMMdd-HHmmss.SSS");
        System.out.println("Now is: "+df.format(now));
    }   
}
