package mammoth.server;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class WriteThread implements Runnable {

    private ServerConf conf;
    private BlockingQueue<WriteTask> tasks;
    private String set;
    private ConcurrentHashMap<String, BlockingQueue<WriteTask>> sq;

    public WriteThread(ServerConf conf, String set, ConcurrentHashMap<String, BlockingQueue<WriteTask>> sq) {
        this.conf = conf;
        this.set = set;
        this.sq = sq;
        this.tasks = sq.get(set);
    }


    @Override
    public void run() {
        try {
            StorePhoto sp = new StorePhoto(conf);
            while (true) {
//				WriteTask t = tasks.take();
                //该线程退出条件，60秒内没有新的任务，或者在删除该集合时，手动插入一个md5为空的任务
                WriteTask t = tasks.poll(60L, TimeUnit.SECONDS);

                if (t == null || t.getMd5() == null) {
                    sq.remove(set);
                    break;
                }
//				synchronized (t) {
//					t.setResult(sp.storePhoto(t.getSet(), t.getMd5(), t.getContent(), t.getCoff(), t.getClen(),""));
//					t.notify();
//				}
                sp.storePhoto(t.getSet(), t.getMd5(), t.getContent(), t.getClen(), t.getCoff(), t.getFname());
            }
            sp.close();
            System.out.println(Thread.currentThread() + "writethread 结束");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
