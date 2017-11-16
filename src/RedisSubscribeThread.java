/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

import java.io.PrintStream;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 *
 * @author Pankaj
 */
public class RedisSubscribeThread implements Runnable {

    RedisSubscribe subscriber;
    String topic;
    JedisPool marketdatapool;

    RedisSubscribeThread(RedisSubscribe subscriber, String topic) {
        this.subscriber = subscriber;
        this.topic = topic;
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        //jedisPoolConfig.setMaxWaitMillis(60000);
        jedisPoolConfig.setMaxWaitMillis(2);
        marketdatapool = new JedisPool(jedisPoolConfig, "127.0.0.1", 6379, 2000, null, 9);

    }

    @Override
    public void run() {
        try (Jedis jedis = marketdatapool.getResource()) {
            if (!topic.contains("*")) {
                jedis.subscribe(subscriber, topic);
            } else {
                jedis.psubscribe(subscriber, topic);
            }
        }
    }
}
