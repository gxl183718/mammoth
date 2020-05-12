package mammoth.server;

import redis.clients.jedis.JedisPubSub;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

class MMSPubSub extends JedisPubSub {
    private String logDir = "log/";
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

    public MMSPubSub(ServerConf conf) {
    }

    public void onMessage(String channel, String message) {
        String s = df.format(new Date());
        String logFileName = "mm.info." + s + ".log";
        FileWriter fw = null;

        try {
            fw = new FileWriter(new File(logDir + logFileName), true);
            BufferedWriter w = new BufferedWriter(fw);
            w.write(channel);
            w.write(":");
            w.write(message);
            w.write("\n");
            w.close();
            fw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void onSubscribe(String channel, int subscribedChannels) {
    }

    public void onUnsubscribe(String channel, int subscribedChannels) {
    }

    public void onPSubscribe(String pattern, int subscribedChannels) {
    }

    public void onPUnsubscribe(String pattern, int subscribedChannels) {
    }

    public void onPMessage(String pattern, String channel,
                           String message) {
        String s = df.format(new Date());
        String logFileName = "mm.info." + s + ".log";
        FileWriter fw = null;

        try {
            fw = new FileWriter(new File(logDir + logFileName), true);
            BufferedWriter w = new BufferedWriter(fw);
            w.write(channel);
            w.write(":");
            w.write(message);
            w.write("\n");
            w.close();
            fw.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}