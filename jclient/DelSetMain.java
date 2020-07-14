package mammoth.jclient;

import redis.clients.jedis.Jedis;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DelSetMain {
    public static void main(String[] args) {
        long zzqSecond = 24 * 60 * 60;
        ClientAPI clientAPI = new ClientAPI();
        try {
            clientAPI.init("STL://10.148.28.1:26379;10.148.28.2:26379;10.148.28.3:26379;10.148.28.4:26379;10.148.28.5:26379");
        } catch (Exception e) {
            e.printStackTrace();
        }
        DeleteSet deleteSet = new DeleteSet(clientAPI);

        Timer timer = new Timer();
        timer.schedule(new DelTask(zzqSecond, deleteSet), 1000, 24*60*60*1000);

    }
    private static String getTime(){
        return DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()) + " ----> ";
    }
    static class DelTask extends TimerTask {
        long zzqSecond;
        DeleteSet deleteSet;
        public DelTask(long zzqSecond, DeleteSet deleteSet) {
            this.zzqSecond = zzqSecond;
            this.deleteSet = deleteSet;
        }
        @Override
        public void run() {
            long time = System.currentTimeMillis()/1000 - zzqSecond;
            System.out.println(getTime() + "时间节点 " + time);
            Jedis jedis = new Jedis("10.148.28.200", 30099);
            Set<String> zzqs = jedis.keys("*zq*");
            jedis.close();
            TreeSet<String> zzqst = new TreeSet<>(zzqs);
            for (String zzq : zzqst) {
                long set = Long.parseLong(zzq.substring(4));
                String key = zzq.substring(1);
                if (set < time){
                    try {
                        System.out.println(getTime() + key + " start");
                        deleteSet.delSet(key);
                        System.out.println(getTime() + key + " end");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
