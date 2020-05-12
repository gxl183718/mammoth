package mammoth.server;

import java.util.concurrent.atomic.AtomicLong;

public class ServerProfile {
    public static AtomicLong writtenBytes = new AtomicLong(0);            //一共写入的字节数,单位字节
    public static AtomicLong readBytes = new AtomicLong(0);
    public static AtomicLong readDelay = new AtomicLong(0);                //总读取延迟，单位毫秒
    public static AtomicLong readN = new AtomicLong(0);                    //读取次数
    public static AtomicLong readErr = new AtomicLong(0);
    public static AtomicLong writeN = new AtomicLong(0);
    public static AtomicLong writeErr = new AtomicLong(0);
    public static AtomicLong queuedIndex = new AtomicLong(0);
    public static AtomicLong handledIndex = new AtomicLong(0);
    public static AtomicLong ignoredIndex = new AtomicLong(0);
    public static AtomicLong completedIndex = new AtomicLong(0);

    public static void addWrite(int n) {
        writtenBytes.addAndGet(n);
        writeN.incrementAndGet();
    }

    public static void updateRead(long rbytes, long latency) {
        readBytes.addAndGet(rbytes);
        readDelay.addAndGet(latency);
        readN.incrementAndGet();
    }

    public static void reset() {
        readDelay.set(0);
        readN.set(0);
    }
}
