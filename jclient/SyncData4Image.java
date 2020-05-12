package mammoth.jclient;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class SyncData4Image {
    private static ClientAPI clientAPIsz = new ClientAPI();
    private static ClientAPI clientAPIbj = new ClientAPI();

    private static volatile LinkedBlockingQueue<String> queue0 = new LinkedBlockingQueue();
    public static void main(String[] args){
        try {
            clientAPIsz.init("STL://10.144.16.56:26379;10.144.16.57:26379");
            System.out.println("sz init over!");
            clientAPIbj.init("STL://10.136.132.78:26379;10.136.132.79:26379");
            System.out.println("bj init over!");

        } catch (Exception e) {
            e.printStackTrace();
        }

        new Thread(){
            @Override
            public void run() {
                Jedis jedis =new Jedis("10.144.16.56", 30099);
                if(queue0.size() == 0){
                    Set<String> set = jedis.hkeys("ds.set");
                    for(String s : set){
                        if (s.startsWith("i"))
                            queue0.add(s);
                    }
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("queue write size =  " + queue0.size());
                }else{
                    try {
                        System.out.println("queue size else " + queue0.size());
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }.start();

        for(int a = 0; a < Integer.parseInt(args[0]); a ++){
            new Thread(){
                @Override
                public void run() {
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    super.run();
                    Jedis jedis =new Jedis("10.144.16.57", 30088);
                    String key;
                    while(true){
                        try {
                            System.out.println("while ture");
                            key = queue0.take();
                            System.out.println(key + "--> start");

                            byte[] szbs = clientAPIsz.get(key);
                            System.out.println("put--> start");

                            clientAPIbj.put(key, szbs);
                            jedis.hdel("ds.set", key);
                            System.out.println(key + "--> ok!");
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();
        }


    }
}
