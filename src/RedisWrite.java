/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.text.DecimalFormat;
import java.util.logging.Logger;
import redis.clients.jedis.Jedis;

/**
 *
 * @author Pankaj
 */
public class RedisWrite {

    String value;
    long time;
    String key;
    private static final Logger logger = Logger.getLogger(RedisWrite.class.getName());

   
        public RedisWrite(String value, long time, String duration, String displayName, String type) {
        this.time = time;
        this.key=displayName+":"+duration+":"+type;
        Pair v=new Pair();
        v.setTime(time);
        v.setValue(value);
        Gson gson = new GsonBuilder()
            .create();
       this.value = gson.toJson(v);
        
    }

    public void write() {
          RedisSubscribe.jedis.zadd(key, time, value);         
    }
    
        public String roundToDecimal(String input) {
        if (!input.equals("")) {
            Float inputvalue = Float.parseFloat(input);
            DecimalFormat df = new DecimalFormat("0.00");
            df.setMaximumFractionDigits(2);
            return df.format(inputvalue);
        } else {
            return input;
        }
    }
}
