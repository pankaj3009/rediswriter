
import java.io.PrintStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jquantlib.time.BusinessDayConvention;
import org.jquantlib.time.DateParser;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.ScanResult;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
/**
 *
 * @author Pankaj
 */
public class RedisSubscribe extends JedisPubSub {

   public static JedisPool subscribepool;
   public static JedisPool writepool;
   private static final Logger logger = Logger.getLogger(RedisSubscribe.class.getName());
   public static Jedis jedis;
    public RedisSubscribe(String redisSubscribeIP, int redisSubscribePort,int db, String topic) {
        try {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
//            jedisPoolConfig.setMaxWaitMillis(60000);
            jedisPoolConfig.setMaxWaitMillis(1000);
            subscribepool = new JedisPool(jedisPoolConfig, redisSubscribeIP,redisSubscribePort, 10000, null, db);
            writepool = new JedisPool(jedisPoolConfig, Entry.redisWriteIP,Entry.redisWritePort, 10000, null, Entry.redisWriteDB);
            jedis=writepool.getResource();
            
            Thread t = new Thread(new RedisSubscribeThread(this, topic));
            t.setName("Redis Market Data Subscriber");
            t.start();
            JedisPool deletepool = new JedisPool(jedisPoolConfig, Entry.redisWriteIP,Entry.redisWritePort, 2000, null, Entry.redisWriteDB);
            String purgeDate=getPriorBusinessDay(new SimpleDateFormat("yyyy-MM-dd").format(new Date()),"yyyy-MM-dd",Entry.purgeage);
            long purgeThreshold=new SimpleDateFormat("yyyy-MM-dd").parse(purgeDate).getTime();
            Jedis deleteJedis=deletepool.getResource();
             String cursor = "";
            while (!cursor.equals("0")) {
                cursor = cursor.equals("") ? "0" : cursor;
                   ScanResult s = deleteJedis.scan(cursor);
                    cursor = s.getCursor();
                    for (Object key : s.getResult()) {
                        if (key.toString().contains(":")) {
                               deleteJedis.zremrangeByScore(key.toString(), 0, purgeThreshold);
                        }
                    }
                
            }
            if(deleteJedis!=null){
                deleteJedis.close();
            }
            deletepool.destroy();
        } catch (Exception e) {
            logger.log(Level.SEVERE, null, e);
        }
    }
    
    public static String getPriorBusinessDay(String date, String outputFormat, int ref) {
        String reference = date;
        for (int i = 0; i < ref; i++) {
            SimpleDateFormat sdfOutput = new SimpleDateFormat(outputFormat);
            org.jquantlib.time.Date today = DateParser.parseISO(reference);
            org.jquantlib.time.Date yesterday = today.sub(1);
            yesterday = Entry.ind.adjust(yesterday, BusinessDayConvention.Preceding);
            String yesterdayString = (sdfOutput.format(yesterday.isoDate()));
            reference = yesterdayString;
        }
        return reference;
    }

    @Override
    public void onMessage(String channel, String message) {
        super.onMessage(channel, message); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
        super.onPMessage(pattern, channel, message); //To change body of generated methods, choose Tools | Templates.
        //System.out.println(message);
        String[] messageA = message.split(",", -1);
        long date = Long.parseLong(messageA[1]);
        String value = messageA[2];
        String displayName = messageA[3];
        String[] symbol = displayName.split("_", -1);
        int type = Integer.parseInt(messageA[0]);
        String tickType = null;

        switch (type) {
            case TickType.BID_SIZE: //bidsize
                break;
            case TickType.BID: //bidprice
                tickType = "bid";
                break;
            case TickType.ASK://askprice
                tickType = "ask";
                break;
            case TickType.ASK_SIZE: //ask size
                break;
            case TickType.LAST: //last price
                tickType = "close";
                break;
            case TickType.LAST_SIZE: //last size
                tickType = "size";
                break;
            case TickType.HIGH:
                break;
            case TickType.LOW:
                break;
            case TickType.VOLUME: //volume
                tickType = "dayvolume";
                break;
            case TickType.CLOSE: //close

                break;
            case TickType.OPEN: //open
                break;
            case 99:
                break;
            default:
                break;
        }
        if (tickType != null) {
            new RedisWrite(value, date, "tick",displayName,tickType).write();
        }
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        super.onSubscribe(channel, subscribedChannels); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        super.onUnsubscribe(channel, subscribedChannels); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
        super.onPUnsubscribe(pattern, subscribedChannels); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
        super.onPSubscribe(pattern, subscribedChannels); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void unsubscribe() {
        super.unsubscribe(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void unsubscribe(String... channels) {
        super.unsubscribe(channels); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void subscribe(String... channels) {
        super.subscribe(channels); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void psubscribe(String... patterns) {
        super.psubscribe(patterns); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void punsubscribe() {
        super.punsubscribe(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void punsubscribe(String... patterns) {
        super.punsubscribe(patterns); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public boolean isSubscribed() {
        return super.isSubscribed(); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void proceedWithPatterns(Client client, String... patterns) {
        super.proceedWithPatterns(client, patterns); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void proceed(Client client, String... channels) {
        super.proceed(client, channels); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int getSubscribedChannels() {
        return super.getSubscribedChannels(); //To change body of generated methods, choose Tools | Templates.
    }
}
