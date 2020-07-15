package tach.converter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.PrintStream;

import static java.lang.Integer.parseInt;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author tmcglynn
 */
public class LineProcessor {
        
    String  savedKey;
    String  savedVal;
    boolean lastSave; 
    PreparedStatement insert;
    int notice;
    int line;
    
    int wrote = 0;
    
    public void initialize(int nid, Connection conn) throws Exception {
        notice = nid;
        insert = conn.prepareStatement("insert into details(nid, line, key, textval, realval, arrval) values(?,?,?,?,?,?)");        
    }
    
    
    
    
    public int linesWritten() {
        return wrote;
    }
    
    public void closeStmt() {
        try {
            if (insert != null) {
                insert.close();
            }
        } catch (Exception e) {
            System.err.println("Error closing insert statment");
        }
    }
    public void processLine(String input, int lineNo) {
        line = lineNo;
        if (!lastSave) {
            savedKey = null;
            savedVal = null;                    
        }
        lastSave = false;
        
        input = input.trim();
        if (input.length() > 0) {
            char c1 = input.charAt(0);
            if (c1 == ' ' || c1 == '#') {
                processContOrComm(input);
            } else {
                String[] fields = input.split(":", 2);
                if (fields.length == 1) {
                    processSingleton(input);
                } else {
                    String key = fields[0].trim();
                    String val = fields[1].trim();
                    
                    switch (key) {
                          
                        case "TITLE":
                        case "NOTICE_TYPE":
                        case "RECORD_NUM":
                        case "TRIGGER_NUM":
                        case "SOLN_STATUS":
                        case "BURST_ID":
                        case "LOC_QUALITY":
                        case "HARD_RATIO":
                        case "LC_URL":
                        case "ENERGY_BAND":
                        case "SRC_ID_NUM":
                        case "LOC_URL":
                        case "POS_MAP_URL":
                        case "HEALPIX_URL":
                        case "MAP_URL":
                        case "ALGORITHM":
                        case "ERROR":
                        case "TRANS_NUM":
                        case "FULL_ID_NUM":
                        case "TRIGGER_ID":
                        case "MISC":
                        case "SOURCE_OBJ":
                        case "REF_NUM":
                        case "TRANS_OBJ":
                        case "TRIGGER_FLAGS":
                        case "EVENT_ID_NUM":
                        case "SRC_NAME":
                        case "SRC_CLASS":
                        case "EXPT":
                        case "TGT_NAME":
                        case "INST_MODES":
                        case "MERIT":
                        case "TRIGGER_INDEX":
                        case "FLAGS":
                        case "FD_URL":
                        case "EXPOSURE_ID":
                        case "X_GRB_POS":
                        case "Y_GRB_POS":
                        case "BINNING_INDEX":
                        case "IM_URL":
                        case "WAIT_SEC":
                        case "OBS_SEC":
                        case "SLEW_QUERY":
                        case "N_BINS":
                        case "TERM_COND":
                        case "N_PKTS":
                        case "N_EVTS":
                        case "SPER_URL":
                        case "N_STARS":
                        case "DET_THRESH":
                        case "PHOTO_THRESH":
                        case "SL_URL":
                        case "BKG_MEAN":
                        case "WAVEFORM":
                        case "TAM[0-3]":
                        case "AMPLIFIER":
                        case "SPEC_URL":
                        case "MERIT_PARAMS":
                        case "GRB_POS_XRT_Y":
                        case "GRB_POS_XRT_Z":
                        case "IMAGE_URL":
                        case "GAIN":
                        case "TP_URL":
                        case "REGION_SOURCE":
                        case "ERROR_CODE":
                        case "ALARM_CODE":
                        case "PARAM1":
                        case "PARAM2":
                        case "PARAM3":
                        case "PARAM4":
                        case "PARAM5":
                        case "PARAM6":
                        case "ID_NUM":
                        case "ERROR_NUM":
                        case "STATUS":
                        case "ACS":
                        case "NOTICE_SERNUM":
                        case "PACKAGE_CODE":
                        case "CATALOG_NUM":
                        case "SAMPLING":
                        case "SRC_TYPE":

                            if (val.startsWith("Undef")) {
                                ignore(key,val);
                            } else {
                                emit(key, val);
                            }
                            break;
                            
                        case "CURR_DATE":
                        case "GRB_DATE":
                        case "START_DATE":
                        case "END_DATE":
                        case "EVT_DATE":
                        case "TRIGGER_DATE":
                        case "DISCOVERY_DATE":                            
                        case "TRANS_DATE":
                        case "POINT_DATE":
                        case "OUTBURST_DATE":
                        case "SLEW_DATE":
                        case "EVENT_DATE":
                        case "SRC_DATE":
                        case "MAX_DATE":
                        case "MAP_DATE":
                        case "IMG_START_DATE":
                        case "LC_START_DATE":
                        case "LC_STOP_DATE":
                        case "SPER_START_DATE":
                        case "SPER_STOP_DATE":
                        case "SPEC_START_DATE":
                        case "SPEC_STOP_DATE":
                        case "TP_START_DATE":
                        case "TP_END_DATE":
                            tjdToMJD(key, val);
                            break;
                            
                        case "NOTICE_DATE":
                            processDate(key, val);
                            break;
                            
                        case "CURR_POINT_RA":
                        case "CURR_POINT_DEC":
                        case "POINT_RA":
                        case "POINT_DEC":
                        case "GRB_RA":
                        case "GRB_DEC":
                        case "SRC_RA":
                        case "SRC_DEC":
                        case "TRANS_RA":
                        case "TRANS_DEC":
                        case "RA":
                        case "DEC":
                        case "WXM_CNTR_RA":
                        case "WXM_CNTR_DEC":
                        case "SXC_CNTR_RA":
                        case "SXC_CNTR_DEC":
                        case "NEXT_POINT_RA":
                        case "NEXT_POINT_DEC":
                        case "EVENT_RA":
                        case "EVENT_DEC":
                        case "REGION_RA":
                        case "REGION_DEC":
                            processCoord(key, val);
                            break;
                            
                        case "SC_RA":
                        case "SC_DEC":
                            shortCoord(key, val);
                            break;
                            
                            
                        case "SC_LON_LAT":
                        case "ISS_LON_LAT":
                            lonLat(key, val);
                            break;

                        case "DELTA_TIME":
                        case "MOON_DIST":
                        case "GRB_ERROR":
                        case "GRB_INTEN_TOT":
                        case "INTEG_DUR":
                        case "FIRST_PHOTON":
                        case "LAST_PHOTON":
                        case "GRB_PHI":
                        case "GRB_THETA":
                        case "TEMP_TEST_STAT":
                        case "IMAGE_TEST_STAT":
                        case "GRB_INTEN":
                        case "DATA_SIGNIF":
                        case "INTEG_TIME":
                        case "DATA_TIME_SCALE":
                        case "LOC_ALGORITHM":
                        case "EVT_RA":
                        case "EVT_DEC":
                        case "EVT_ERROR":
                        case "SIGNIFICANCE":
                        case "FOREGND_DUR":
                        case "BACKGND_DUR":
                        case "BACKGND_DUR1":
                        case "BACKGND_DUR2":
                        case "SUN_DIST":
                        case "TRIGGER_SIGNIF":
                        case "DATA_INTERVAL":
                        case "TRIGGER_DUR":
                        case "TRANS_DURATION":
                        case "TRANS_ERROR":
                        case "EARTH_ANGLE":
                        case "OBS_TIME":
                        case "WXM_MAX_SIZE":
                        case "SXC_RATE":
                        case "GAMMA_RATE":
                        case "SXC_MAX_SIZE":
                        case "SXC_LOC_SN":
                        case "SC_LONG":
                        case "WXM_LOC_SN":
                        case "EVENT_FLUX":
                        case "EVENT_ERROR":
                        case "SRC_ERROR":
                        case "SRC_FLUX":
                        case "PEAK_MAG":
                        case "LEAD_TIME":
                        case "MAX_UNCERT": 
                        case "EVT_FLUENCE":
                        case "EVT_DUR":
                        case "EVENT_FLUENCE":
                        case "EVENT_DUR":
                        case "RATE_SIGNIF":
                        case "IMAGE_SIGNIF":
                        case "WAIT_TIME":
                        case "TOT_EXPO_TIME":
                        case "LIVETIME":
                        case "EXPO_TIME":
                        case "TP_EXPO_TIME":
                        case "REGION_ERROR":
                        case "EVENT_PHI":
                        case "EVENT_THETA":
                        case "EVENT_INTEN":
                        case "SIGNIF":
                        case "BASELINE_FLUX":
                        case "SC_-Z_RA":   // Not right tokens for standard positions
                        case "SC_-Z_DEC":
                            firstFieldDouble(key, val);
                            break;
                            
                        case "RELIABILITY":
                        case "TYPE_CLASS":
                        case "SPECTRAL_CLASS":
                        case "TIME_SCALE":
                        case "X_OFFSET":
                        case "Y_OFFSET":
                        case "WIDTH":
                        case "HEIGHT": 
                        case "SC_REPLY":
                        case "FILTER":
                        case "X_MAX":
                        case "Y_MAX":
                        case "MODE":
                        case "LRPD_BIAS":
                        case "BKG_DUR":
                        case "BKG_INTEN":
                        case "FOSCSAFEPT":
                            firstFieldInt(key, val);
                            break;
                            
                        case "TRIGGER_DET":
                            threeFields(key, val);
                            break;
                            
                        case "GRB_INTEN1":
                        case "GRB_INTEN2":
                        case "GRB_INTEN3":
                        case "GRB_INTEN4":
                            grbInten1(key, val);
                            break;
                            
                        case "GRB_SIGNIF":
                            grbSignif(key, val);
                            break;
                            
                        case "CURR_TIME":
                        case "GRB_TIME":
                        case "EVT_TIME":
                        case "START_TIME":
                        case "END_TIME":
                        case "TRIGGER_TIME":
                        case "DISCOVERY_TIME":
                        case "TRANS_TIME":
                        case "POINT_TIME":
                        case "OUTBURST_TIME":
                        case "SLEW_TIME":
                        case "EVENT_TIME":
                        case "SRC_TIME":
                        case "MAX_TIME":
                        case "MAP_TIME":
                        case "LC_STOP_TIME":
                        case "SPEC_STOP_TIME":
                        case "BKG_TIME":
                            secOfDay(key,val);
                            break;
                            
                        case "SUN_POSTN":
                        case "MOON_POSTN":
                            processPos(key, val);
                            break;
                            
                        case "MOON_ILLUM":
                            percent(key,val);
                            break;
                            
                        case "FUTURE_RA_DEC":
                            ephem1(key,val);
                            break;
                            
                        case "COMMENTS":
                            comment(key, val);
                            break;
                            
                        case "DETECTORS":
                            removeCommas(key, val, 14);
                            break;
                            
                        case "MOST_LIKELY":
                        case "2nd_MOST_LIKELY":
                            probID(key,val);                        
                            break;
                            
                        case "CURR_FLUX":
                        case "BASE_FLUX":
                        case "CUSP_WIDTH":
                        case "BASE_MAG":
                        case "GRB_MAG":
                        case "u0":
                            plusMinus(key, val);
                            break;
                            
                        case "CONTRIB_1":
                        case "CONTRIB_2":
                        case "CONTRIB_3":
                        case "CONTRIB_4":
                        case "CONTRIB_5":
                            coincidence(key, val);
                            break;            
                        
                        case "WXM_CORNER1":
                        case "WXM_CORNER2":
                        case "WXM_CORNER3":
                        case "WXM_CORNER4":
                        case "SXC_CORNER1":
                        case "SXC_CORNER2":
                        case "SXC_CORNER3":
                        case "SXC_CORNER4":
                            twoReals(key, val);
                            break;
                            
                        case "TRIGGER_SOURCE":
                            triggerSource(key, val);
                            break;
                            
                        case "WXM_IMAGE_SN":
                        case "WXM_LC_SN":
                            xySN(key, val);
                            break;
                            
                        case "BAND_FLUX":
                            twoRealCommaSep(key,val);
                            break;
                            
                            
                        case "WXM_SIG/NOISE":
                            snString(key, val);
                            break;
                            
                            
                        case "EVENT_EBAND":
                        case "SRC_EBAND":
                            eband(key, val);
                            break;
                            
                        case "EVENT_TSCALE":
                        case "SRC_TSCALE":
                            timeScale(key, val);
                            break;
                            
                        case "NEXT_POINT_ROLL":
                        case "CURR_POINT_ROLL":
                        case "POINT_ROLL":
                        case "ROLL":
                            degrees(key, val);
                            break;
                            
                        case "AMPLIFICATION":
                        case "MAX_MAG":
                            amp(key, val);
                            break;
                                    
                        case "TGT_NUM":
                            targInfo(key, val);
                            break;
                            
                        case "FOSCSLEWABORT":
                            slewAbort(key, val);
                            break;

                        case "IMG_START_TIME":
                        case "LC_START_TIME":
                        case "SPER_START_TIME":
                        case "SPEC_START_TIME":
                        case "TP_START_TIME":
                            secPlusDelta(key, val);
                            break;
                            
                            
                        case "LC_LIVE_TIME":
                            liveTime(key, val);
                            break;
                            
                        case "SPER_STOP_TIME":
                            stopWithDelta(key, val);
                            break;

                        case "CENTROID_X":
                        case "CENTROID_Y":
                            centroid(key, val);
                            break;
                            
                        case "PH2_ITER":
                        case "COUNTS":
                            intWithLimit(key, val);
                            break;
                            
                            
                        case "STD_DEV":
                        case "MAG_LIMIT":
                            stdWithLimit(key,val);
                            break;
                            
                        case "ENERGY_RANGE":
                            range(key, val);
                            break;
                            
                        case "E_RANGE":
                            erange(key, val);
                            break;
                            
                        case "GAL_COORDS":
                        case "ECL_COORDS":
                        case "LABELS":
                            ignore(key, val);
                            break;

                        default:
                            processUnknown(key, val);
                    }
                }
            }
        } else {
        }
    }

    void tjdToMJD(String key, String val) {
        val = val.replace("(yy/mm/dd)", "");  // Sometimes at end
        val = val.trim();
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 5)) {
            double tjd = Integer.parseInt(flds[0])  + 40000;
            emit(key, ""+tjd);
        }
    }
    
    void processDate(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 6)) {
            double mjd = Utils.mjd(
                    parseInt(flds[3]), flds[2], parseInt(flds[1]), flds[4]);
            if (mjd == mjd) {
                emit(key, "" + mjd);
            } else {
                error(key + " has invalid date:" + val);
            }
        }

    }
    
    void processCoord(String key, String val) {
        if (val.startsWith("Undefined")) {
            ignore(key, val);
            return;
        }
        try {
            String[] flds = val.split("\\s+");
            if (tokenCheck(key, val, flds, 5)) {
                flds[0] = flds[0].replaceAll("d$", "");
                emit(key, "" + Double.parseDouble(flds[0]));
            }
        } catch (Exception e) {
            error(key + " with invalid format:" + val);
        }
    }
    
    void shortCoord(String key, String val) {
        try {
            String[] flds = val.split("\\s+");
            if (tokenCheck(key, val, flds, 3)) {
                emit(key, "" + Double.parseDouble(flds[0]));
            }
        } catch (Exception e) {
            error(key + " with invalid format:" + val);
        }
    }
    
    void lonLat(String key, String val) {
        val = val.replace(",", " ");
        String [] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 3)) {
            emit(key, flds[0]+" "+flds[1]);
        } else {
        }
    }

    void firstFieldDouble(String key, String val) {
        if (val.startsWith("Undefined")  || val.startsWith("<unknown>")) {
            ignore(key, val);
            return;
        }
        try {
            String[] flds = val.split("\\s+");
            emit(key, "" + Double.parseDouble(flds[0]));
        } catch (Exception e) {
            error(key + " with invalid format:" + val);
        }
    }
    
    void firstFieldInt(String key, String val) {
        try {
            String[] flds = val.split("\\s+");
            if (flds[0].endsWith(",")) {
                flds[0] = flds[0].substring(0,flds[0].length()-1);
            }
            emit(key, "" + Long.parseLong(flds[0]));
        } catch (Exception e) {
            error(key + " with invalid format:" + val);
        }
    }
    
    void threeFields(String key, String val) {
        String[] flds = val.split("\\s+");
        if (flds.length == 3  || flds.length == 6) {
            emit(key, flds[0]+" "+flds[1]+" "+flds[2]);            
        } else {
            error(key+" with invalid format:"+val);
        }
    }
    
    void grbInten1(String key, String val) {
        try {
            // Removing this here means that we have constant number of tokens.
            val = val.replace("(", "");            
            String[] flds = val.split("\\s+");
            if (tokenCheck(key, val, flds, 4)) {
                
                double cnts = Double.parseDouble(flds[0]);
                String[] range = flds[2].split("-");
                double r1 = Double.parseDouble(range[0]);
                double r2;
                if (range[1].equals("up")) {
                    r2 = 100000;
                } else {
                    r2 = Double.parseDouble(range[1]);
                }
                double factor = 1;
                if (flds[3].charAt(0) == 'G') {
                    factor = 1000;
                }
                // Range in MeV
                emit(key, cnts+" "+r1*factor+" "+r2*factor);                
            }
        } catch (Exception e) {
            error(key + " with invalid format:" + val);
            
        }
    }
    
    void grbSignif(String key, String val) {
        try {
            String[] flds= val.split("\\s+");
            if (flds.length == 3) {
                emit(key, flds[0]+" "+flds[1]);
            } else if (flds.length == 2) {
                emit(key, flds[0]);
            }
        } catch (Exception e) {
            error(key+" with invalid format:"+val);
        }
    } 


    void secOfDay(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 4)) {
            double sec = Double.parseDouble(flds[0]);
            emit(key, ""+(sec/86400));
        }
    }
    
    void processPos(String key, String val) {
        try {
            String[] flds = val.split("\\s+");
            if (tokenCheck(key, val, flds, 8)) {
                if (flds.length != 8) {
                    error(key + " with wrong number of fields:" + val);
                }
                double c1 = Double.parseDouble(flds[0]);
                double c2 = Double.parseDouble(flds[4]);
                emit(key, "" + c1 + " " + c2);
            }
        } catch (Exception e) {
            error(key + " with invalid format:" + val);
        }
    }

    void percent(String key, String val) {
        String[] flds = val.split("\\s+");
        emit(key, ""+Double.parseDouble(flds[0])/100);
    }
    
    void ephem1(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 8)) {
            flds[0] = flds[0].replaceAll("d$", "");
            flds[1] = flds[1].replaceAll("d$", "");
            emit(key, flds[0] + " " + flds[1] + " " + flds[3] + " " + flds[4]);
        }
    }
    
    void comment(String key, String val) {
        emit(key, val.trim());
    }

    
    
    void removeCommas(String key, String val, int count) {
        String newVal = val.replace(",", " ").trim();
        String[] flds = newVal.split("\\s+");
        if (flds.length == count) {
            emit(key, val);
        } else {
        }
    }
    
    void probID(String key, String val) {
        String[] flds = val.split("%");
        String percent = flds[0].trim();
        if (flds[0].startsWith("Error")) {
            ignore(key, val);
            return;
        } else if (flds[0].startsWith("Below")) {
            emit(key, "-1");
            return;
        }
        try {
            double frac = Double.parseDouble(percent)/100.;
            emit(key, frac+" '"+flds[1].trim()+"'");
        } catch (Exception e) {
            error(key+" field format error.  Expecting double got:"+val);
        }
    }

    void plusMinus(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 4)) {
            emit(key, flds[0]+" "+flds[2]);
        }
    }    
    
    void coincidence(String key, String val) {
        String kflds[] = key.split("\\_");
        String[] flds = val.split("\\s+");
        int item = Integer.parseInt(kflds[1]);
        if (tokenCheck(key, val, flds, 9)) {
            emit("TFLAG_"+item, flds[0]);
            emit("PFLAG_"+item, flds[1]);
            emit("MISSION_INST_"+item, flds[2]);
            emit("NOTICE_TYPE_"+item, flds[3]);
            emit("TRIG_NUM_"+item, flds[4]);
            emit("TRIGGER_MJD_"+item, ""+(Integer.parseInt(flds[5])+40000));
            emit("TRIGGER_TOD"+item, flds[6]);
            emit("TRIGGER_RA_"+item, flds[7]);
            emit("TRIGGER_DEC_"+item, flds[8]);
        }
    }
    
    void twoReals(String key, String val) {
        String[] flds = val.split("\\s+");
        if (flds.length < 2) {
            emit(key, ""+Double.parseDouble(flds[0])+" "+Double.parseDouble(flds[1]));
        }
    }
    
    void triggerSource(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 6)) {
            String[] rng = flds[3].split("-");
            emit(key, ""+Double.parseDouble(rng[0])+" "+Double.parseDouble(rng[1]));
        }
    }
    void  snString(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 7)) {
            emit(key, ""+Double.parseDouble(flds[0])+" "+Double.parseDouble(flds[4]));
        }
    }
    
    void xySN(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 5)) {
            emit(key, ""+Double.parseDouble(flds[1])+" "+Double.parseDouble(flds[3]));
        }
        
    }
    
    void twoRealCommaSep(String key, String val) {
        val = val.replace(",", " ");
        String[] flds = val.split("\\s+");
        if (tokenCheck(key,val, flds, 7)) {
            String rng = flds[5];
            String[] rngs = rng.split("-");
            emit(key, Double.parseDouble(flds[0])+" "+Double.parseDouble(flds[1])+" "+rngs[0]+" "+rngs[1]);
        }        
    }
    
    void eband(String key, String val) {
        val = val.trim();
        if (val.startsWith("Undef")) {
            ignore(key,val);
            return;
        }
        String[] flds = val.split("\\s++");
        String[] rng = flds[1].split("-");
        emit(key, rng[0]+" "+rng[1]);
    }
    
    void timeScale(String key, String val) {
        val = val.trim();
        if (val.startsWith("Undef")) {
            ignore(key, val);
            return;
        }
        if (val.endsWith("day")  || val.endsWith("d")) {
            emit(key, ""+Double.parseDouble(val.substring(0,val.length()-3))*86400);
        } else if (val.endsWith("orbit")) {
            emit(key, ""+Double.parseDouble(val.substring(0,val.length()-5))*86400*3);            
        } else if (val.endsWith("s")) {
            emit(key, val.replace("s", ""));
        } else {
            error("funnyTimescale:"+key+val);
        }
    }
    
    void amp(String key, String val) {
        val = val.trim();
        if (val.startsWith("No longer")) {
            ignore(key, val);
        } else {
            if (val.indexOf("+/-") > 0) {
                plusMinus(key,val);
            } else {
                firstFieldDouble(key, val);
            }
        }
    }
    
    void targInfo(String key, String val) {
        val = val.replace(",", " ");
        String[] flds = val.split("\\s+");
        if (tokenCheck(key,val, flds, 3)) {
            emit(key, Long.parseLong(flds[0])+" "+Long.parseLong(flds[2]));
        }        
    }
    
    void degrees(String key, String val) {
        String[] flds = val.split("\\s+");
        if (flds[0].endsWith("d")) {
            String angle = flds[0].replace("d", "");
            emit(key, ""+Double.parseDouble(angle));
        }
        
    }
    
    void slewAbort(String key, String val) {
        String[] flds = val.split("\\s+");
        flds[0] = flds[0].replace(",", "");
        emit(key, flds[0]+" "+flds[2]);        
    }
    
    void secPlusDelta(String key, String val) {
        String[] flds = val.split("\\s+");
        if (flds.length > 8) {
            
            double sec = Double.parseDouble(flds[0]);
            String delta = flds[flds.length-6];
            emit(key, (sec/86400)+" "+delta);
        }
    }
    
    
    void liveTime(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 3)) {
            String percent = flds[2].replace("%", "");
            emit(key, flds[0]+" "+Double.parseDouble(percent)/100.);
        }
        
    }
    
    void stopWithDelta(String key, String val) {
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 7)) {
            emit(key, Double.parseDouble(flds[0])/86400+" "+flds[5]);
        }
    }
    
    void intWithLimit(String key, String val) {
        val = val.replace("=", " ");
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 3)) {
            emit(key, flds[0]+" "+flds[2]);
        }
    }

    void centroid(String key, String val) {
        val = val.replace("=", " ");
        val = val.replace(",", " ");
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 4)) {
            emit(key, flds[0]+" "+flds[2]);
        }        
    }
    
    void stdWithLimit(String key, String val) {
        val = val.replace("=", " ");
        String[] flds = val.split("\\s+");
        if (tokenCheck(key, val, flds, 4)) {
            emit(key, flds[0]+" "+flds[2]);
        }        
    }
    
    void range(String key, String val) {
        String[] flds = val.split("\\s+");
        String[] rng = flds[0].split("-");
        emit(key, flds[0]+" "+flds[1]);
        
    }
    
    void erange(String key, String val) {
        // [Fermi] There are two different formats, one of which gives both channel
        // and kev ranges and the other which give's kev ranges only.
        // We will put the kev data first.
        String[] flds = val.split("\\s+");
        if (flds[1].equals("[chan]")) {
            String[] crange = flds[0].split("-");
            String[] kevrange = flds[2].split("-");
            if (kevrange[1].equals("inifinity")) {
                kevrange[1] = "1.d99";
            }
            emit(key, kevrange[0]+" "+kevrange[1]+" "+crange[0]+" "+crange[1]);
        } else {
            emit(key, flds[0]+" "+flds[2]);            
        }
    }
    
    void processContOrComm(String line) {
        normal("Comment", line.substring(8));
    }

    void processSingleton(String line) {
        normal("Singleton", line);
    }

    void processUnknown(String key, String val) {
        error("UnknownKey:"+key, val);
    }

    void emit(String key, String val) {
        normal(key, val);
    }
    
    void normal(String key, String val) {
        processDetail(key, val);
    }
    
    void error(String key, String val) {
        System.err.println("Unable to handle: "+key+" -> "+val);
    }
    
    void error(String err) {
        error("Error", err);
    }

    boolean tokenCheck(String key, String val, String[] flds, int expected) {
        if (flds.length != expected) {
            error(key + " with incorrect token count:" + val + " expected token count: " + expected + ", got:" + flds.length);
            return false;
        }
        return true;
    }

    void ignore(String key, String val) {
        error("Ignore", key);
    }
    
    void processDetail(String key, String val) {
        
        if (val == null) {
            val = "";
        }
        
        String[] flds = val.split("\\s+");
        double[] arr  = null;
        boolean numeric = flds.length > 0;
        if (numeric) {
            arr = new double[flds.length];
            for (int i=0; i<flds.length; i += 1) {
                try {
                    arr[i] = Double.parseDouble(flds[i]);
                } catch (Exception e) {
                    numeric= false;
                    break;
                }
            }
        }
        try {
            insert.setInt(1, notice);
            insert.setInt(2, line);
            insert.setString(3, key);
            insert.setString(4, val);
            if (numeric) {
               insert.setDouble(5, arr[0]);
            } else {
                insert.setNull(5, java.sql.Types.DOUBLE);
            }
            if (numeric && flds.length > 1) {
                insert.setObject(6, arr);
            } else {
                insert.setNull(6, java.sql.Types.ARRAY);
            }
            boolean hasRS = insert.execute();
            if (hasRS || insert.getUpdateCount() != 1) {
                System.err.println("Invalid update");
            } else {
                wrote += 1;
            }
        } catch (Exception e) {
            System.err.println("Unable to insert value for notice/line:"+notice+"/"+line+"\n"+e);
        }
    }
}
