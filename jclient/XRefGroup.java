package mammoth.jclient;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class XRefGroup {
    public static class XRef {
        int idx;    // orignal index in keys list
        int seqno;
        String key;
        byte[] value = null;

        public XRef() {
            seqno = -1;
            key = null;
        }

        public XRef(int idx, String key, AtomicInteger curseqno) {
            this.idx = idx;
            this.key = key;
            this.seqno = curseqno.incrementAndGet();
        }

        public String toString() {
            return "ID " + idx + " SEQNO " + seqno + " KEY " + key;
        }
    }

    private ClientConf conf;
    private int gid;
    private long bts = 0;
    private AtomicInteger nr = new AtomicInteger(0);
    private ConcurrentHashMap<Integer, XRef> toWait = new ConcurrentHashMap<Integer, XRef>();
    private ConcurrentHashMap<Integer, XRef> fina = new ConcurrentHashMap<Integer, XRef>();

    public XRefGroup(ClientConf conf, Map<Integer, XRefGroup> wmap, AtomicInteger curgrpno) {
        this.conf = conf;
        gid = curgrpno.incrementAndGet();
        wmap.put(gid, this);
    }

    public void addToGroup(XRef xref) {
        bts = System.currentTimeMillis();
        toWait.put(xref.seqno, xref);
        nr.incrementAndGet();
    }

    public boolean isTimedout() {
        if (System.currentTimeMillis() - bts >= conf.getMgetTimeout()) {
            return true;
        } else
            return false;
    }

    public void doneXRef(Integer seqno, byte[] value) {
        XRef x = toWait.remove(seqno);
        if (x != null) {
            x.value = value;
            fina.put(seqno, x);
        }
    }

    public boolean waitAll() {
        return toWait.isEmpty();
    }

    public boolean waitAny() {
        return !fina.isEmpty();
    }

    public Collection<XRef> getAll() {
        Collection<XRef> tmp = null;

        if (fina.size() == nr.get()) {
            tmp = fina.values();
            fina.clear();
        }
        return tmp;
    }

    public long getGroupSize() {
        return nr.get();
    }

    public long getAvailableSize() {
        return fina.size();
    }

    public int getGid() {
        return gid;
    }

    public AtomicInteger getNr() {
        return nr;
    }

    public ConcurrentHashMap<Integer, XRef> getFina() {
        return fina;
    }

    public ConcurrentHashMap<Integer, XRef> getToWait() {
        return toWait;
    }

    public void setBts(long bts) {
        this.bts = bts;
    }
}
