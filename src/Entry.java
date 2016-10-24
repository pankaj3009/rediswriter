/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.File;
import java.io.FileInputStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

/**
 *
 * @author Pankaj
 */
public class Entry {

    private static final Logger logger = Logger.getLogger(Entry.class.getName());
    public static String FutureMetric = "india.nse.future.s1.tick";
    public static String EquityMetric = "india.nse.equity.s1.tick";
    public static String OptionMetric = "india.nse.option.s1.tick";
    public static String redisWriteIP;
    public static int redisWritePort;
    public static int redisWriteDB;

    public static void main(String[] args) {
        if (args.length == 1) {
            Properties globalProperties = loadParameters(args[0]);
            String redisSubscribeIP = globalProperties.getProperty("redissubscribeip", "127.0.0.1");
            int redisSubscribePort = Integer.valueOf(globalProperties.getProperty("redissubscribeport", "6379"));
            redisWriteIP = (globalProperties.getProperty("rediswriteip", "127.0.0.1"));
            redisWritePort = Integer.valueOf(globalProperties.getProperty("rediswriteport", "6379"));
            redisWriteDB = Integer.valueOf(globalProperties.getProperty("rediswritedb", "9"));
            String topic = globalProperties.getProperty("topic", "*");
            FutureMetric = globalProperties.getProperty("futuremetric");
            EquityMetric = globalProperties.getProperty("equitymetric");
            OptionMetric = globalProperties.getProperty("optionmetric");
            String endTimeStr = globalProperties.getProperty("endtime", "15:35:00");
            java.util.Date currDate = new java.util.Date();
            DateFormat df = new SimpleDateFormat("yyyyMMdd");
            String currDateStr = df.format(currDate);
            String endDateStr = currDateStr + " " + endTimeStr;
            java.util.Date endDate = parseDate("yyyyMMdd HH:mm:ss", endDateStr);
            Timer endProcessing = new Timer("Timer: " + " EndProcessing");
            endProcessing.schedule(end, endDate);
            new RedisSubscribe(redisSubscribeIP, redisSubscribePort, 9, topic);
        } else {
            logger.log(Level.SEVERE, "Please specify an unambiguous parameter file");
        }
    }
    static TimerTask end = new TimerTask() {
        @Override
        public void run() {
            System.exit(0);
        }
    };

    public static java.util.Date parseDate(String format, String date) {
        java.util.Date dt = null;
        try {
            SimpleDateFormat sdf1 = new SimpleDateFormat(format);
            dt = sdf1.parse(date);
        } catch (Exception e) {
            logger.log(Level.INFO, "101", e);
        }
        return dt;
    }

    public static Properties loadParameters(String parameterFile) {
        Properties p = new Properties();
        FileInputStream propFile;
        File f = new File(parameterFile);
        if (f.exists()) {
            try {
                propFile = new FileInputStream(parameterFile);
                p.load(propFile);

            } catch (Exception ex) {
                logger.log(Level.INFO, "101", ex);
            }
        }

        return p;
    }
}
