package mammoth.jclient;

import mammoth.jclient.PhotoClient.SocketHashEntry.SEntry;
import mammoth.jclient.XRefGroup.XRef;
import mammoth.common.RedisPool;
import mammoth.common.RedisPoolSelector;
import mammoth.common.RedisPoolSelector.RedisConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PhotoClient {
    private ClientConf conf;
    private static RedisPool rpL1;
    private static RedisPoolSelector rps;
    private static AtomicInteger curgrpno = new AtomicInteger(0);
    private static AtomicInteger curseqno = new AtomicInteger(0);
    private Map<Integer, XRefGroup> wmap = new ConcurrentHashMap<Integer, XRefGroup>();
    private long ckpt_ts = 0;
    private long ss_id = -1;
    private long ls_id = -1;

    public RedisPool getRpL1() {
        return rpL1;
    }

    public RedisPoolSelector getRPS() {
        return rps;
    }

    // 缓存与服务端的tcp连接,服务端名称到连接的映射
    public static class SocketHashEntry {
        String hostname;
        int port, cnr;
        AtomicInteger xnr = new AtomicInteger(0);
        Map<Long, SEntry> map;
        AtomicLong nextId = new AtomicLong(0);

        public static class SEntry {
            public Socket sock;
            public long id;
            public boolean used;
            public DataInputStream dis;
            public DataOutputStream dos;

            public SEntry(Socket sock, long id, boolean used,
                          DataInputStream dis, DataOutputStream dos) {
                this.sock = sock;
                this.id = id;
                this.used = used;
                this.dis = dis;
                this.dos = dos;
            }
        }

        public SocketHashEntry(String hostname, int port, int cnr) {
            this.hostname = hostname;
            this.port = port;
            this.cnr = cnr;
            this.map = new ConcurrentHashMap<Long, SEntry>();
        }

        public void setFreeSocket(long id) {
            SEntry e = map.get(id);
            if (e != null) {
                e.used = false;
            }
            synchronized (this) {
                this.notify();
            }
        }

        public boolean probSelected() {
            if (map.size() > 0)
                return true;
            else {
                // 1/100 prob selected
                if (new Random().nextInt(100) == 0)
                    return true;
                else
                    return false;
            }
        }

        public long getFreeSocket() throws IOException {
            boolean found = false;
            long id = -1;

            do {
                synchronized (this) {
                    for (SEntry e : map.values()) {
                        if (!e.used) {
                            // ok, it is unused
                            found = true;
                            e.used = true;
                            id = e.id;
                            break;
                        }
                    }
                }

                if (!found) {
                    if (map.size() + xnr.getAndIncrement()< cnr) {
                        // do connect now
                        Socket socket = new Socket();
//                        xnr.getAndIncrement();
                        try {
                            socket.connect(new InetSocketAddress(this.hostname,
                                    this.port));
                            socket.setTcpNoDelay(true);
                            id = this
                                    .addToSocketsAsUsed(
                                            socket,
                                            new DataInputStream(socket
                                                    .getInputStream()),
                                            new DataOutputStream(socket
                                                    .getOutputStream()));
                            // new DataInputStream(new
                            // BufferedInputStream(socket.getInputStream())),
                            // new DataOutputStream(new
                            // BufferedOutputStream(socket.getOutputStream())));
                            {//认证信息
                                byte[] bytes = new byte[2];
                                bytes[0] = (byte) ClientConf.userName.length();
                                bytes[1] = (byte) ClientConf.passWord.length();
                                socket.getOutputStream().write(bytes);
                                socket.getOutputStream().write((ClientConf.userName + ClientConf.passWord).getBytes());
                                socket.getOutputStream().flush();
                                DataInputStream dis = new DataInputStream(socket.getInputStream());
                                int num = dis.readInt();
                                if (num == -1) {
                                    System.out.println("认证失败，客户端与服务端版本不一致！！");
                                    socket.close();
                                    break;
                                }
                                else System.out.println("认证成功！");
                            }


                            System.out.println("[GFS] New connection @ " + id
                                    + " for " + hostname + ":" + port);
                        } catch (SocketException e) {
                            xnr.getAndDecrement();
                            System.out.println("[GFS] Connect to " + hostname
                                    + ":" + port + " failed w/ "
                                    + e.getMessage());
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e1) {
                            }
                            throw e;
                        } catch (Exception e) {
                            xnr.getAndDecrement();
                            System.out.println("[GFS] Connect to " + hostname
                                    + ":" + port + " failed w/ "
                                    + e.getMessage());
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e1) {
                            }
                            throw new IOException(e.getMessage());
                        }
                        xnr.getAndDecrement();
                    } else {
                        xnr.getAndDecrement();
                        do {
                            try {
                                synchronized (this) {
                                    // System.out.println("wait ...");
                                    this.wait(60000);
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                continue;
                            }
                            break;
                        } while (true);
                    }
                } else {
                    break;
                }
            } while (id == -1);

            return id;
        }

        public long addToSocketsAsUsed(Socket sock, DataInputStream dis,
                                       DataOutputStream dos) {
            SEntry e = new SEntry(sock, nextId.getAndIncrement(), true, dis,
                    dos);
            synchronized (this) {
                map.put(e.id, e);
            }
            return e.id;
        }

        public void addToSockets(Socket sock, DataInputStream dis,
                                 DataOutputStream dos) {
            SEntry e = new SEntry(sock, nextId.getAndIncrement(), false, dis,
                    dos);
            synchronized (this) {
                map.put(e.id, e);
            }
        }

        public void useSocket(long id) {
            synchronized (this) {
                SEntry e = map.get(id);
                if (e != null) {
                    e.used = true;
                }
            }
        }

        public void delFromSockets(long id) {
            System.out.println("Del sock @ " + id + " for " + hostname + ":"
                    + port);
            SEntry e = null;
            synchronized (this) {
                e = map.get(id);
                map.remove(id);
                this.notifyAll();
            }
            if (e != null) {
                try {
                    e.dis.close();
                    e.dos.close();
                    e.sock.close();
                } catch (IOException e1) {
                }
            }
        }

        public void clear() {
            synchronized (this) {
                for (Entry<Long, SEntry> e : map.entrySet()) {
                    try {
                        e.getValue().sock.close();
                    } catch (IOException e1) {
                    }
                }
            }
        }
    }

    ;

    public class XSearchResult {
        public String redirect_info = null;
        public byte[] result = null;
        public long offset = 0;
        public int length = 0;

        public XSearchResult() {
        }
    }

    private ConcurrentHashMap<String, SocketHashEntry> socketHash = new ConcurrentHashMap<String, SocketHashEntry>();
    private ConcurrentHashMap<String, SocketHashEntry> igetSH = new ConcurrentHashMap<String, SocketHashEntry>();
    private Map<Long, String> servers = new ConcurrentHashMap<>();

    public Map<Long, String> getServers() {
        return servers;
    }

    public PhotoClient() {
        conf = new ClientConf();
    }

    public PhotoClient(ClientConf conf) {
        this.conf = conf;
    }

    public void init() throws Exception {
        switch (conf.getRedisMode()) {
            case SENTINEL:
                rpL1 = new RedisPool(conf, "l1.master");
                break;
            case STANDALONE:
                rpL1 = new RedisPool(conf, "nomaster");
                break;
        }
        rps = new RedisPoolSelector(conf, rpL1);
    }

    public ClientConf getConf() {
        return conf;
    }

    public void setConf(ClientConf conf) {
        this.conf = conf;
    }

    public void addToServers(long id, String server) {
        servers.put(id, server);
    }

    public void clearServers(){
        servers.clear();
    }

    public Map<String, SocketHashEntry> getSocketHash() {
        return socketHash;
    }

    public void setSocketHash(
            ConcurrentHashMap<String, SocketHashEntry> socketHash) {
        this.socketHash = socketHash;
    }

    public List<String> getActiveMMSByHB() {
        List<String> ls = new ArrayList<>();
        Jedis jedis = rpL1.getResource();

        try {
            //替换慢查询(keys("mm.hb.*");
            Set<String> allNodes = jedis.zrange("mm.active", 0, -1);
            for (String node : allNodes) {
                if (jedis.exists("mm.hb." + node))
                    ls.add(node);
            }
        } catch (JedisException e) {
            System.out.println("Get mm.hb.* failed: " + e.getMessage());
        } finally {
            rpL1.putInstance(jedis);
        }

        return ls;
    }

    private byte[] __handleInput(DataInputStream dis) throws IOException {
        int count;

        synchronized (dis) {
            count = dis.readInt();
            switch (count) {
                case -1:
                    return null;
                default: {
                    return readBytes(count, dis);
                }
            }
        }
    }

    private byte[] __handleInput_info(DataInputStream dis) throws IOException {
        int count;

        synchronized (dis) {
            count = dis.readInt();
            switch (count) {
                case -1:
                    return null;
                default: {
                    return readBytes(count, dis);
                }
            }
        }
    }

    private XSearchResult __handleInput4XSearch(DataInputStream dis)
            throws IOException {
        int count;

        synchronized (dis) {
            count = dis.readInt();
            switch (count) {
                case -1:
                    return null;
                default: {
                    XSearchResult xsr = new XSearchResult();

                    if (count > 0)
                        xsr.result = readBytes(count, dis);
                    else {
                        xsr.redirect_info = new String(readBytes(-count, dis));
                        xsr.offset = dis.readLong();
                        xsr.length = dis.readInt();
                    }
                    return xsr;
                }
            }
        }
    }

    private ResultSet __handleInput4ResultSet(DataInputStream dis)
            throws IOException {
        int count;

        synchronized (dis) {
            count = dis.readInt();
            switch (count) {
                case -1:
                    return null;
                default: {
                    ObjectInputStream ois = new ObjectInputStream(dis);
                    try {
                        return (ResultSet) ois.readObject();
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        return null;
                    }
                }
            }
        }
    }

    private void addHeat(String set, String md5) {
        Jedis jedis = null;
        try {
            jedis = getRpL1().getResource();
            int period = Integer.parseInt(jedis.hget("mm.client.conf", "heat"));
            String key = "h." + set + "@" + md5;
            Pipeline pi = jedis.pipelined();
            pi.set(key, "1");
            pi.expire(key, period);
            pi.sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rpL1.putInstance(jedis);
        }
    }

    private String getiport(String id) {
        Jedis jedis = getRpL1().getResource();
        if (jedis == null)
            throw new JedisException("Get default jedis instance failed.");

        String iport;
        try {
            iport = jedis.hget("mm.iport", id);
        } finally {
            jedis.close();
            rpL1.putInstance(jedis);
        }
        return iport;
    }

    protected Set<String> getServerIds(String key) {
        RedisConnection rc = null;
        Set<String> targets = new TreeSet<String>();
        String set = key.split("@")[0];
        String md5 = key.split("@")[1];
        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            if (jedis != null) {
                String info = jedis.hget(set, md5);
                if ('#' == info.charAt(1))
                    info = info.substring(2);
                String[] infos = info.split("#");
                for (int i = 0; i < infos.length; i++) {
                    targets.add(getiport(infos[i].split("@")[2]));
                }
                return targets;
            }
        } catch (JedisConnectionException e) {
            System.out.println(set + "@" + md5
                    + ": Jedis connection broken in getPhoto");
        } catch (JedisException e) {
            System.out.println(set + "@" + md5
                    + ": Jedis exception in getPhoto: " + e.getMessage());
        } catch (Exception e) {
            System.out.println(set + "@" + md5 + ": Exception in getPhoto: "
                    + e.getMessage());
        } finally {
            rps.putL2(rc);
        }
        return null;
    }

    private long getSid(String errnode) {
        Jedis jedis = getRpL1().getResource();
        if (jedis == null)
            throw new JedisException("Get default jedis instance failed.");

        Long sid;
        try {
            // reget the sid
            Map<String, String> dnsMap = jedis.hgetAll("mm.dns");
            for (Entry<String, String> e : dnsMap.entrySet()) {
                if (e.getValue().equals(errnode))
                    errnode = e.getKey();
            }
            sid = jedis.zscore("mm.active", errnode).longValue();
        } finally {
            jedis.close();
            rpL1.putInstance(jedis);
        }
        return sid;
    }

    protected void delThisInfo(String[] keys, String errnode) {
        RedisConnection rc = null;
        String info;
        String newInfo = "0";
        try {
            long serverId = getSid(errnode);
            rc = rps.getL2(keys[0], false);
            Jedis jedis = rc.jedis;
            info = jedis.hget(keys[0], keys[1]);
            if ('#' == info.charAt(1)) {
                info = info.substring(2);
            }
            for (String s : info.split("#")) {
                if (serverId != Integer.parseInt(s.split("@")[2])) {
                    newInfo += ("#" + s);
                }
            }
            jedis.hset(keys[0], keys[1], newInfo);
        } catch (JedisConnectionException e) {
            System.out.println(keys[0] + "@" + keys[1]
                    + ": Jedis connection broken in getPhoto");
        } catch (JedisException e) {
            System.out.println(keys[0] + "@" + keys[1]
                    + ": Jedis exception in getPhoto: " + e.getMessage());
        } catch (Exception e) {
            System.out.println(keys[0] + "@" + keys[1] + ": Exception in getPhoto: "
                    + e.getMessage());
        } finally {
            rps.putL2(rc);
        }
    }

    private String __syncStorePhoto(String set, String md5, byte[] content,
                                    SocketHashEntry she, long fLen, String fn) throws IOException {
        long id = she.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not find free socket for server: "
                    + she.hostname + ":" + she.port);
        DataOutputStream storeos = she.map.get(id).dos;
        DataInputStream storeis = she.map.get(id).dis;
        byte[] header = new byte[4];
        header[0] = ActionType.SYNCSTORE;
        header[1] = (byte) set.length();
        header[2] = (byte) md5.length();
        byte[] r = null;
        try {
            synchronized (storeos) {
                storeos.write(header);
                storeos.writeInt(content.length);
		        if (fn != null) {
                    storeos.writeInt(fn.getBytes().length);
                    storeos.write(fn.getBytes());
               	} else {
                    storeos.writeInt(0);
               	}
                // set,md5,content的实际内容写过去
                storeos.write(set.getBytes());
                storeos.write(md5.getBytes());
                storeos.write(content);
                storeos.flush();
            }
            r = __handleInput_info(storeis);
            she.setFreeSocket(id);
        } catch (Exception e) {
            System.out.println("__syncStore send/recv failed: "
                    + e.getMessage() + " r?null=" + (r == null ? true : false));
		e.printStackTrace();
            // remove this socket do reconnect?
            she.delFromSockets(id);
        }
        if (r == null) {
            // if we can't get reasonable response from redis, report it!
            String rr = null;
            RedisConnection rc = null;

            try {
                rc = rps.getL2(set, true);
                Jedis jedis = rc.jedis;
                if (jedis != null)
                    rr = jedis.hget(set, md5);
            } catch (JedisConnectionException e) {
                System.out.println(set + "@" + md5
                        + ": Jedis connection broken in __syncStoreObject");
            } catch (JedisException e) {
                System.out.println(set + "@" + md5 + ": Jedis exception: "
                        + e.getMessage());
            } catch (Exception e) {
                System.out.println(set + "@" + md5 + ": Exception: "
                        + e.getMessage());
            } finally {
                rps.putL2(rc);
            }
            if (rr == null)
                throw new IOException(
                        "MM Server failed or Metadata connection broken?");
            return rr;
        }

        String s = new String(r, "US-ASCII");

        if (s.startsWith("#FAIL:")) {
            throw new IOException("MM server failure: " + s);
        }
        return s;
    }

    public FileInfo getFileInfo(String key) throws IOException {
        FileInfo fileInfo = new FileInfo();
        String info = null;
        RedisConnection rc = null;
        String set = key.split("@")[0];
        String md5 = key.split("@")[1];
        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            if (jedis != null) {
                if (jedis.hexists(set, md5)) {
                    info = jedis.hget(set, md5);
                } else {
                    return getFileInfoFromSS(set, md5); // Xsearch--getPhoto(MMS)
                }
            }
        } catch (JedisConnectionException e) {
            System.out.println(set + "@" + md5
                    + ": Jedis connection broken in getPhoto");
        } catch (JedisException e) {
            System.out.println(set + "@" + md5
                    + ": Jedis exception in getPhoto: " + e.getMessage());
        } catch (Exception e) {
            System.out.println(set + "@" + md5 + ": Exception in getPhoto: "
                    + e.getMessage());
        } finally {
            rps.putL2(rc);
        }
        if (info == null)
            throw new IOException(set + "@" + md5 + " doesn't exist in MM server or connection broken;");
        else {
            if ('#' == info.charAt(1))
                info = info.substring(2);
	    info = info.split("#")[0];
            fileInfo.setLength(Long.parseLong(info.split("@|#")[5]));
            if (info.split("@").length == 7){
            	fileInfo.setInputTime(0);
            	fileInfo.setFileName(null);
            } else if (info.split("@").length == 9){
            	fileInfo.setInputTime(Long.parseLong(info.split("@|#")[7]));
            	fileInfo.setFileName(info.split("@|#")[8]);
            }
	    return fileInfo;
        }
    }

    private FileInfo getFileInfoFromSS(String set, String md5) throws IOException {
        FileInfo fileInfo = new FileInfo();
        SocketHashEntry searchSocket = null;
        String[] sers = new String[2];
        sers[0] = servers.get(ss_id);
        sers[1] = servers.get(ls_id);
        String ser = null;
        for (String server : sers) {
            if (server == null) {
                continue;
            }
            try {
                searchSocket = socketHash.get(server);
                searchSocket = getSocketHashEntry(searchSocket, server);
            } catch (IOException e) {
                System.out.println("Invalid server name or port: " + server + " -> " + e.getMessage());
                continue;
            }
            ser = server;
            break;
        }
        if (ser == null) {
            throw new IOException("Secondary Server idx " + ss_id + " and " + ls_id
                    + " can't be resolved.");
        }
        // action, set, md5
        byte[] header = new byte[4];
        header[0] = ActionType.GETINFO;
        header[1] = (byte) set.getBytes().length;
        header[2] = (byte) md5.getBytes().length;
        long id = searchSocket.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not get free socket for server: "
                    + ser);

        String info = null;
        try {
            searchSocket.map.get(id).dos.write(header);

            searchSocket.map.get(id).dos.writeBytes(set);
            searchSocket.map.get(id).dos.writeBytes(md5);
            searchSocket.map.get(id).dos.flush();

            info = new String(__handleInput4GetInfo(searchSocket.map.get(id).dis));
            searchSocket.setFreeSocket(id);
        } catch (Exception e) {
            e.printStackTrace();
            // remove this socket do reconnect?
            searchSocket.delFromSockets(id);
        }

        if (info == null) {
            throw new IOException("Invalid response from mm server:" + ser);
        } else {
            if ('#' == info.charAt(1))
                info = info.substring(2);
            fileInfo.setLength(Long.parseLong(info.split("@|#")[5]));
            fileInfo.setInputTime(Long.parseLong(info.split("@|#")[7]));
            fileInfo.setFileName(info.split("@|#")[8]);
            return fileInfo;
        }
    }

    private byte[] __handleInput4GetInfo(DataInputStream dis) throws IOException {
        int count;

        synchronized (dis) {
            count = dis.readInt();
            switch (count) {
                case -1:
                    return null;
                default: {
                    return readBytes(count, dis);
                }
            }
        }
    }

    private void __asyncStorePhoto(String set, String md5, byte[] content,
                                   SocketHashEntry she) throws IOException {
        long id = she.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not get free socket for server: "
                    + she.hostname + ":" + she.port);
        DataOutputStream storeos = she.map.get(id).dos;

        // action,set,md5,content的length写过去
        byte[] header = new byte[4];
        header[0] = ActionType.ASYNCSTORE;
        header[1] = (byte) set.length();
        header[2] = (byte) md5.length();

        try {
            synchronized (storeos) {
                storeos.write(header);
                storeos.writeInt(content.length);

                // set,md5,content的实际内容写过去
                storeos.write(set.getBytes());
                storeos.write(md5.getBytes());
                storeos.write(content);
                storeos.flush();
            }
            she.setFreeSocket(id);
        } catch (Exception e) {
            e.printStackTrace();
            // remove this socket do reconnect?
            she.delFromSockets(id);
        }
    }

    /**
     * 同步写
     *
     * @param set
     * @param md5
     * @param content
     * @return
     */
    public String syncStorePhoto(String set, String md5, byte[] content,
                                 SocketHashEntry she, boolean nodedup, long fLen, long off, String fn) throws IOException {
        if (conf.getMode() == ClientConf.MODE.NODEDUP || nodedup) {
            return __syncStorePhoto(set, md5, content, she, fLen, fn);
        } else if (conf.getMode() == ClientConf.MODE.DEDUP) {
            String info = null;
            RedisConnection rc = null;
            try {
                rc = rps.getL2(set, true);
                Jedis jedis = rc.jedis;
                if (jedis != null)
                    info = jedis.hget(set, md5);
                if (info != null && fLen > 0) {
//                    if (conf.isLogDupInfo())
//                        jedis.hincrBy("mm.dedup.info", set + "@" + md5, 1);
                    return info;
                }else {
                    rc.rp.getBalanceTarget().incrementAndGet();
                }
            } catch (JedisConnectionException e) {
                System.out.println(set + "@" + md5
                        + ": Jedis connection broken in syncStorePhoto");
            } catch (JedisException e) {
                System.out.println(set + "@" + md5
                        + ": Jedis exception in syncStorePhoto: "
                        + e.getMessage());
            } catch (Exception e) {
                System.out.println(set + "@" + md5
                        + ": Exception in syncStorePhoto: " + e.getMessage());
            } finally {
                rps.putL2(rc);
            }
            return __syncStorePhoto(set, md5, content, she, fLen, fn);
        }
        throw new IOException(
                "Invalid Operation Mode: either NODEDUP or DEDUP.");
    }

    public void asyncStorePhoto(String set, String md5, byte[] content,
                                SocketHashEntry she) throws IOException {
        if (conf.getMode() == ClientConf.MODE.NODEDUP) {
            __asyncStorePhoto(set, md5, content, she);
        } else if (conf.getMode() == ClientConf.MODE.DEDUP) {
            String info = null;
            RedisConnection rc = null;

            try {
                rc = rps.getL2(set, true);
                Jedis jedis = rc.jedis;
                if (jedis != null)
                    info = jedis.hget(set, md5);

                if (info == null) {
                    __asyncStorePhoto(set, md5, content, she);
                } else {
                    // NOTE: log the dup info
                    if (conf.isLogDupInfo())
                        jedis.hincrBy("mm.dedup.info", set + "@" + md5, 1);
                }
            } catch (JedisConnectionException e) {
                System.out.println(set + "@" + md5
                        + ": Jedis connection broken in asyncStorePhoto");
            } catch (JedisException e) {
                System.out.println(set + "@" + md5 + ": Jedis exception: "
                        + e.getMessage());
            } catch (Exception e) {
                System.out.println(set + "@" + md5 + ": Exception: "
                        + e.getMessage());
            } finally {
                rps.putL2(rc);
            }
        } else {
            throw new IOException(
                    "Invalid Operation Mode: either NODEDUP or DEDUP.");
        }
    }

    // 批量存储时没有判断重复
    public String[] mPut(String set, String[] md5s, byte[][] content,
                         SocketHashEntry she) throws IOException {
        long id = she.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not find free socket for server: "
                    + she.hostname + ":" + she.port);
        DataOutputStream dos = she.map.get(id).dos;
        DataInputStream dis = she.map.get(id).dis;

        int n = md5s.length;
        String[] r = new String[n];
        byte[] header = new byte[4];
        header[0] = ActionType.MPUT;
        header[1] = (byte) set.length();
        try {
            dos.write(header);
            dos.writeInt(n);
            dos.write(set.getBytes());
            for (int i = 0; i < n; i++) {
                dos.writeInt(md5s[i].getBytes().length);
                dos.write(md5s[i].getBytes());
            }
            for (int i = 0; i < n; i++)
                dos.writeInt(content[i].length);
            for (int i = 0; i < n; i++)
                dos.write(content[i]);

            // BUG-XXX: 注意，此处for循环中也还需要处理dis.readInt()的返回值！
            int count = dis.readInt();
            if (count == -1)
                throw new IOException("MM server failure.");
            r[0] = new String(readBytes(count, dis));
            for (int i = 1; i < n; i++)
                r[i] = new String(readBytes(dis.readInt(), dis));
            she.setFreeSocket(id);
        } catch (Exception e) {
            e.printStackTrace();
            // remove this socket do reconnect?
            she.delFromSockets(id);
        }
        return r;
    }

    private boolean isInRedis(String this_set) {
        long this_ts = Long.MAX_VALUE;

        try {
            if (!Character.isDigit(this_set.charAt(0))) {
                this_set = this_set.substring(1);
            }
            this_ts = Long.parseLong(this_set);
        } catch (Exception e) {
        }
        if (this_ts > ckpt_ts)
            return true;
        else
            return false;
    }


    private byte[] getPhotoFromSS(String set, String md5, long offset, int length) throws IOException {
        SocketHashEntry searchSocket = null;
        String[] sers = new String[2];
        sers[0] = servers.get(ss_id);
        sers[1] = servers.get(ls_id);
        String ser = null;
        for (String server : sers) {
            if (server == null) {
                continue;
            }
            try {
                searchSocket = socketHash.get(server);
                searchSocket = getSocketHashEntry(searchSocket, server);
            } catch (IOException e) {
                System.out.println("Invalid server name or port: " + server + " -> " + e.getMessage());
                continue;
            }
            ser = server;
            break;
        }
        if (ser == null) {
            throw new IOException("Secondary Server idx " + ss_id + " and " + ls_id
                    + " can't be resolved.");
        }
        // action, set, md5
        byte[] header = new byte[4];
        header[0] = ActionType.XSEARCH;
        header[1] = (byte) set.getBytes().length;
        header[2] = (byte) md5.getBytes().length;
        long id = searchSocket.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not get free socket for server: " + ser);

        XSearchResult xsr = null;
        try {
            searchSocket.map.get(id).dos.write(header);

            searchSocket.map.get(id).dos.writeBytes(set);
            searchSocket.map.get(id).dos.writeBytes(md5);
            searchSocket.map.get(id).dos.writeLong(offset);
            searchSocket.map.get(id).dos.writeInt(length);
            searchSocket.map.get(id).dos.flush();

            xsr = __handleInput4XSearch(searchSocket.map.get(id).dis);
            searchSocket.setFreeSocket(id);
        } catch (Exception e) {
            e.printStackTrace();
            // remove this socket do reconnect?
            searchSocket.delFromSockets(id);
        }

        if (xsr == null) {
            throw new IOException("Invalid response from mm server:" + ser);
        } else if (xsr.result != null) {
            return xsr.result;
        } else if (xsr.redirect_info != null) {
            // we should redirect the request now
            return searchPhoto(xsr.redirect_info);
        } else {
            throw new IOException("Internal error in mm server:" + ser);
        }
    }


    /**
     * 同步取
     *
     * @param set redis中的键以set开头,因此读取图片要加上它的集合名
     * @param md5
     * @return 图片内容, 如果图片不存在则返回长度为0的byte数组
     */
    public byte[] getPhoto(String set, String md5) throws IOException {
        String info = null;
        RedisConnection rc = null;
        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            if (jedis != null) {
                info = jedis.hget(set, md5);
            }
        } catch (JedisConnectionException e) {
            System.out.println(set + "@" + md5
                    + ": Jedis connection broken in getPhoto");
        } catch (JedisException e) {
            System.out.println(set + "@" + md5
                    + ": Jedis exception in getPhoto: " + e.getMessage());
        } catch (Exception e) {
            System.out.println(set + "@" + md5 + ": Exception in getPhoto: "
                    + e.getMessage());
        } finally {
            rps.putL2(rc);
        }
        if (info == null)
            throw new IOException(set + "@" + md5 + " doesn't exist in MM server or connection broken;");
        else
            return searchPhoto(info);
    }

    /**
     * infos是拼接的元信息，各个元信息用#隔开
     */
    byte[] searchPhoto(String infos) throws IOException {
        byte[] r = null;
        for (String info : infos.split("#")) {
            try {
                String[] si = info.split("@");
                r = searchByInfo(info, si);
                if (r.length >= 0)
                    break;
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
        }
        if (r == null)
            throw new IOException("Failed to search MM object: " + infos);
        return r;
    }


    private boolean refreshServers(){
        Jedis jedis = rpL1.getResource();
        if (jedis == null)
            return false;
        try {
            Set<Tuple> newServers = jedis.zrangeWithScores("mm.active", 0, -1);
            synchronized (servers) {
                if (newServers != null && newServers.size() > 0) {
                    clearServers();
                    for (Tuple t : newServers) {
                        addToServers((long) t.getScore(), t.getElement());
                    }
                    return true;
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            getRpL1().putInstance(jedis);
        }
        return false;
    }
    /**
     * info是一个文件的元信息，没有拼接的
     */
    byte[] searchByInfo(String info, String[] infos) throws IOException {
        if (infos.length != conf.getVlen()) {
            throw new IOException("Invalid INFO string, info length is "
                    + infos.length);
        }
        SocketHashEntry searchSocket = null;
        String server = servers.get(Long.parseLong(infos[2]));
        if (server == null){
            //maybe the system added new nodes, we should refresh 'servers'
            //and try it again;
            refreshServers();
            server = servers.get(Long.parseLong(infos[2]));
            if (server == null)
                throw new IOException("Server idx " + infos[2]
                    + " can't be resolved.");
        }

        searchSocket = socketHash.get(server);
        searchSocket = getSocketHashEntry(searchSocket, server);

        // action,info的length写过去
        byte[] header = new byte[4];
        header[0] = ActionType.SEARCH;
        header[1] = (byte) info.getBytes().length;
        long id = searchSocket.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not get free socket for server "
                    + server);

        byte[] r = null;
        try {
            searchSocket.map.get(id).dos.write(header);

            // info的实际内容写过去
            searchSocket.map.get(id).dos.writeBytes(info);
            searchSocket.map.get(id).dos.flush();

            r = __handleInput(searchSocket.map.get(id).dis);
            searchSocket.setFreeSocket(id);
        } catch (Exception e) {
            e.printStackTrace();
            // remove this socket do reconnect?
            searchSocket.delFromSockets(id);
        }
        if (r == null)
            throw new IOException("Internal error in mm server:" + server);
        else
            return r;
    }

    private SocketHashEntry getSocketHashEntry(SocketHashEntry searchSocket, String server) throws IOException {
        if (searchSocket == null) {
            String[] s = server.split(":");
            if (s.length == 2) {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(s[0], Integer
                        .parseInt(s[1])));
                socket.setTcpNoDelay(true);
                searchSocket = new SocketHashEntry(s[0],
                        Integer.parseInt(s[1]), conf.getSockPerServer());
                searchSocket.addToSocketsAsUsed(socket, new DataInputStream(
                                socket.getInputStream()),
                        new DataOutputStream(socket.getOutputStream()));
                byte[] bytes = new byte[2];
                bytes[0] = (byte) ClientConf.userName.length();
                bytes[1] = (byte) ClientConf.passWord.length();
                socket.getOutputStream().write(bytes);
                socket.getOutputStream().write((ClientConf.userName + ClientConf.passWord).getBytes());
                socket.getOutputStream().flush();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                int num = dis.readInt();
                if (num == -1) {
                    System.out.println("认证失败，客户端与服务端版本不一致！！");
                    socket.close();
                }
                else System.out.println("认证成功！");
                SocketHashEntry old = socketHash.putIfAbsent(server,
                        searchSocket);
                if (old != null) {
                    searchSocket.clear();
                    searchSocket = old;
                }
            } else
                throw new IOException("Invalid server name or port: " + server);
        }
        return searchSocket;
    }


    /**
     * 同步删
     *
     * @param set redis中的键以set开头,因此读取图片要加上它的集合名
     * @param md5
     */
    public void deletePhoto(String set, String md5) throws IOException {
        RedisConnection rc = null;
        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            String info = jedis.hget(set, md5);
            if (info == null) {
                jedis.hset(set, md5, "2#");
            } else if (info.split("@")[1] == "#") {
                info = "2" + info.substring(1);
            } else {
                info = "2#" + info;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            rps.putL2(rc);
        }

        SocketHashEntry deleteSocket = null;
        String[] sers = new String[2];
        sers[0] = servers.get(ss_id);
        sers[1] = servers.get(ls_id);
        String ser = null;
        for (String server : sers) {
//            if (server == null) {
//                throw new IOException("Secondary Server idx " + ss_id
//                        + " can't be resolved.");
//            }
            try {
                deleteSocket = socketHash.get(server);
                deleteSocket = getSocketHashEntry(deleteSocket, server);
            } catch (IOException e) {
                continue;
            }
            ser = server;
        }

        // action, set, md5
        byte[] header = new byte[4];
        header[0] = ActionType.DELETE;
        header[1] = (byte) set.getBytes().length;
        header[2] = (byte) md5.getBytes().length;
        long id = deleteSocket.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not get free socket for server: "
                    + ser);

        try {
            deleteSocket.map.get(id).dos.write(header);

            deleteSocket.map.get(id).dos.writeBytes(set);
            deleteSocket.map.get(id).dos.writeBytes(md5);
            deleteSocket.map.get(id).dos.flush();

            deleteSocket.setFreeSocket(id);
        } catch (Exception e) {
            e.printStackTrace();
            // remove this socket do reconnect?
            deleteSocket.delFromSockets(id);
        }
    }

    /**
     * 从输入流中读取count个字节
     *
     * @param count
     * @return
     */
    private byte[] readBytes(int count, InputStream istream) throws IOException {
        byte[] buf = new byte[count];
        int n = 0;

        while (count > n) {
            n += istream.read(buf, n, count - n);
        }

        return buf;
    }

    private OutputStream returnoutput(byte[] buf) {
        OutputStream outputStream = new OutputStream() {

            @Override
            public void write(int b) throws IOException {
                // TODO Auto-generated method stub

            }
        };
        return outputStream;
    }

    /**
     * 关闭流、套接字和与redis的连接 用于读和写的套接字全部都关闭
     */
    public void close() {
        try {
            for (SocketHashEntry s : socketHash.values()) {
                for (SEntry e : s.map.values()) {
                    e.sock.close();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        rps.quit();
        getRpL1().quit();
    }

    public Map<String, String> getNrFromSet(String set) throws IOException {
        RedisConnection rc = null;

        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            if (jedis != null)
                return jedis.hgetAll(set);
        } catch (JedisConnectionException e) {
            e.printStackTrace();
            System.out.println(set + ": Jedis connection broken in getNr");
        } catch (JedisException e) {
            System.out.println(set + ": Jedis exception: " + e.getMessage());
        } catch (Exception e) {
            System.out.println(set + ": Exception: " + e.getMessage());
        } finally {
            rps.putL2(rc);
        }
        throw new IOException(set + ": Jedis Connection broken?");
    }

    public XRefGroup createXRefGroup() {
        return new XRefGroup(conf, wmap, curgrpno);
    }

    public void removeXRefGroup(XRefGroup g) {
        wmap.remove(g.getGid());
    }

    public int __iget(int gid, int seqno, String set, String md5, long alen)
            throws IOException, StopException {
        String info = null;
        RedisConnection rc = null;

        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            if (jedis != null)
                info = jedis.hget(set, md5);
        } catch (JedisConnectionException e) {
            System.out.println(set + "@" + md5
                    + ": Jedis connection broken in __iget");
        } catch (JedisException e) {
            System.out.println(set + "@" + md5 + ": Jedis exception: "
                    + e.getMessage());
        } catch (Exception e) {
            System.out.println(set + "@" + md5 + ": Exception: "
                    + e.getMessage());
        } finally {
            rps.putL2(rc);
        }

        if (info == null) {
            throw new IOException(set + "@" + md5
                    + " doesn't exist in MMM server or connection broken.");
        } else {
            return __igetInfo(gid, seqno, info, alen);
        }
    }

    public int __igetInfo(int gid, int seqno, String infos, long alen)
            throws IOException, StopException {
        boolean r = false;
        int len = 0;

        for (String info : infos.split("#")) {
            try {
                String[] si = info.split("@");

                len = Integer.parseInt(si[5]);
                alen -= len;
                if (alen <= 0) {
                    throw new StopException();
                }
                r = __igetMMObject(gid, seqno, info, si);
                if (r)
                    break;
            } catch (IOException e) {
                System.err.println("GID " + gid + " seqno " + seqno + " infos "
                        + infos + " -> Got IOExcpetion : " + e.getMessage());
                continue;
            }
        }
        if (r)
            return len;
        else
            return -1;
    }

    public boolean __igetMMObject(int gid, int seqno, String info,
                                  String[] infos) throws IOException {
        if (infos.length != conf.getVlen()) {
            throw new IOException("Invalid INFO string, info length is "
                    + infos.length);
        }

        SocketHashEntry igetSocket = null;
        String server = servers.get(Long.parseLong(infos[2]));
        if (server == null)
            throw new IOException("Server idx " + infos[2]
                    + " can't be resolved.");
        igetSocket = igetSH.get(server);
        if (igetSocket == null) {
            String[] s = server.split(":");
            if (s.length == 2) {
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress(s[0], Integer
                        .parseInt(s[1])));
                socket.setTcpNoDelay(true);
                igetSocket = new SocketHashEntry(s[0], Integer.parseInt(s[1]),
                        conf.getSockPerServer());
                igetSocket.addToSocketsAsUsed(
                        socket,
                        new DataInputStream(socket.getInputStream()),
                        // new DataInputStream(new
                        // BufferedInputStream(socket.getInputStream())),
                        new DataOutputStream(new BufferedOutputStream(socket
                                .getOutputStream())));
                igetSH.put(server, igetSocket);
            } else
                throw new IOException("Invalid server name or port: " + server);
        }

        // action,info的length写过去
        byte[] header = new byte[4];
        header[0] = ActionType.IGET;
        header[1] = (byte) info.getBytes().length;
        long id = igetSocket.getFreeSocket();
        if (id == -1)
            throw new IOException("Could not get free socket for server "
                    + server);

        boolean r = true;
        try {
            // do some recv here
            if (igetSocket.map.get(id).dis.available() > 0) {
                try {
                    __doProgress(igetSocket.map.get(id));
                } catch (IOException e1) {
                }
            }

            igetSocket.map.get(id).dos.write(header);
            igetSocket.map.get(id).dos.writeInt(gid);
            igetSocket.map.get(id).dos.writeInt(seqno);

            // info的实际内容写过去
            igetSocket.map.get(id).dos.writeBytes(info);
            igetSocket.map.get(id).dos.flush();

            // try to get a new socket
            if (igetSocket.xnr.get() < igetSocket.cnr) {
                long xid = igetSocket.getFreeSocket();
                igetSocket.setFreeSocket(xid);
            }
            igetSocket.setFreeSocket(id);
        } catch (Exception e) {
            e.printStackTrace();
            // remove this socket do reconnect?
            try {
                __doProgress(igetSocket.map.get(id));
            } catch (IOException e1) {
            }
            igetSocket.delFromSockets(id);
            r = false;
        }

        return r;
    }

    public long iGet(XRefGroup g, int idx, String set, String md5, long alen)
            throws IOException, StopException {
        XRef x = new XRef(idx, set + "@" + md5, curseqno);

        // send it to server
        int len = __iget(g.getGid(), x.seqno, set, md5, alen);
        // put it to group
        if (len >= 0) {
            g.addToGroup(x);
            return x.seqno;
        } else
            throw new IOException("__iget(" + set + "@" + md5 + ") failed.");
    }

    public boolean __doProgress(SEntry se) throws IOException {
        int gid;
        int seqno;
        boolean r = false;

        synchronized (se) {
            if (se.dis.available() > 0) {
                gid = se.dis.readInt();
                if (gid != -1) {
                    seqno = se.dis.readInt();
                    int len = se.dis.readInt();
                    byte[] b = readBytes(len, se.dis);
                    XRefGroup g = wmap.get(gid);
                    if (g != null) {
                        g.doneXRef(seqno, b);
                        r = true;
                    }
                }
            }
        }
        return r;
    }

    public boolean iWaitAll(XRefGroup g) throws IOException {
        do {
            if (g.waitAll() || g.getNr().get() == g.getFina().size())
                return true;

            // progress inputs
            for (String server : servers.values()) {
                SocketHashEntry she = igetSH.get(server);
                if (she != null) {
                    synchronized (she) {
                        for (SEntry se : she.map.values()) {
                            try {
                                if (__doProgress(se))
                                    g.setBts(System.currentTimeMillis());
                            } catch (IOException e1) {
                                e1.printStackTrace();
                                she.delFromSockets(se.id);
                            }
                        }
                    }
                }
            }
            if (g.isTimedout())
                return false;
        } while (true);
    }

    public class StopException extends Exception {
        private static final long serialVersionUID = -7120649613556817964L;
    }

    public List<byte[]> mget(List<String> keys, Map<String, String> cookies)
            throws IOException {
        String bidx_s = cookies.get("idx");
        String alen_s = cookies.get("accept_len");
        int bi = 0, i;
        long alen = 128 * 1024 * 1024;

        if (bidx_s != null)
            bi = Integer.parseInt(bidx_s);
        if (alen_s != null)
            alen = Integer.parseInt(alen_s);
        ArrayList<byte[]> r = new ArrayList<byte[]>(Collections.nCopies(
                keys.size() - bi, (byte[]) null));
        XRefGroup g = createXRefGroup();

        // do info get here
        long begin, end;
        begin = System.nanoTime();
        for (i = bi; i < keys.size(); i++) {
            String key = keys.get(i);
            XRef x = new XRef(i, key, curseqno);
            try {
                for (String info : key.split("#")) {
                    try {
                        String[] si = info.split("@");

                        if (si.length == conf.getVlen()) {
                            alen -= Integer.parseInt(si[5]);
                            if (alen <= 0) {
                                throw new StopException();
                            }
                            if (__igetMMObject(g.getGid(), x.seqno, info, si)) {
                                g.addToGroup(x);
                                break;
                            }
                        } else {
                            int len = __iget(g.getGid(), x.seqno, si[0], si[1],
                                    alen);
                            alen -= len;
                            if (len >= 0) {
                                g.addToGroup(x);
                                break;
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                        continue;
                    }
                }
            } catch (StopException e) {
                break;
            }
        }
        end = System.nanoTime();
        System.out.println(" -> SEND nr " + (i - bi) + " -> "
                + ((end - begin) / 1000.0) + " us.");
        cookies.put("idx", i + "");

        begin = System.nanoTime();
        if (!iWaitAll(g)) {
            System.out.println("Wait XRefGroup " + g.getGid() + " timed out: "
                    + g.getNr().get() + " " + g.getToWait());
        }
        end = System.nanoTime();
        System.out.println(" -> RECV nr " + (i - bi) + " -> "
                + ((end - begin) / 1000.0) + " us.");

        removeXRefGroup(g);

        for (XRef x : g.getFina().values()) {
            r.set(x.idx - bi, x.value);
        }

        return r.subList(0, i - bi);
    }

    public TreeSet<Long> getSets(String prefix) throws IOException {
        TreeSet<Long> tranges = new TreeSet<Long>();
        Jedis jedis = getRpL1().getResource();

        try {
            if (jedis != null) {
                Set<String> keys = jedis.keys("`" + prefix + "*");

                if (keys != null && keys.size() > 0) {
                    for (String key : keys) {
                        key = key.replaceFirst("`", "");
                        key = key.replaceFirst(prefix, "");
                        try {
                            tranges.add(Long.parseLong(key));
                        } catch (NumberFormatException e) {
                        }
                    }
                }
            }
        } catch (JedisException e) {
            System.out.println(prefix + ": Jedis exception: " + e.getMessage());
        } finally {
            getRpL1().putInstance(jedis);
        }

        return tranges;
    }

    public List<String> getSetElements(String set) {
        List<String> r = new ArrayList<String>();
        RedisConnection rc = null;

        try {
            rc = rps.getL2(set, false);
            Jedis jedis = rc.jedis;
            if (jedis != null) {
                if (conf.isGetkeys_do_sort()) {
                    Map<String, String> kvs = jedis.hgetAll(set);
                    TreeMap<String, String> t = new TreeMap<String, String>();

                    for (Entry<String, String> e : kvs.entrySet()) {
                        String[] v = e.getValue().split("@|#");
                        if (v.length >= conf.getVlen()) {
                            t.put(v[6]
                                            + "."
                                            + v[3]
                                            + "."
                                            + (String.format("%015d",
                                    Long.parseLong(v[4]))
                                            + "." + v[2]),
                                    set + "@" + e.getKey());
                        } else {
                            r.add(set + "@" + e.getKey());
                        }
                    }
                    r.addAll(t.values());
                    kvs.clear();
                    t.clear();
                } else {
                    Set<String> t = jedis.hkeys(set);

                    for (String v : t) {
                        r.add(set + "@" + v);
                    }
                    t.clear();
                }
            }
        } catch (JedisConnectionException e) {
            System.out.println(set
                    + ": Jedis connection broken in getSetElements");
        } catch (JedisException e) {
            System.out.println(set + ": Jedis exception: " + e.getMessage());
        } catch (Exception e) {
            System.out.println(set + ": Exception: " + e.getMessage());
        } finally {
            rps.putL2(rc);
        }
        return r;
    }

    /**
     * getAllSets() will not refresh Jedis connection, caller should do it
     *
     * @return
     * @throws IOException
     */
    private TreeSet<String> getAllSets(Jedis jedis) throws IOException {
        TreeSet<String> tranges = new TreeSet<String>();

        Set<String> keys = jedis.keys("`*");

        if (keys != null && keys.size() > 0) {
            for (String key : keys) {
                key = key.replaceAll("`", "");
                tranges.add(key);
            }
        }

        return tranges;
    }

    private class GenValue {
        boolean isGen = false;
        String newValue = null;
    }

    private GenValue __gen_value(String value, Long sid) {
        GenValue r = new GenValue();

        for (String info : value.split("#")) {
            String[] si = info.split("@");
            boolean doClean = false;

            if (si.length >= conf.getVlen()) {
                try {
                    if (Long.parseLong(si[2]) == sid) {
                        // ok, we should clean this entry
                        doClean = true;
                        r.isGen = true;
                    }
                } catch (NumberFormatException nfe) {
                }
            }
            if (!doClean) {
                if (r.newValue == null)
                    r.newValue = info;
                else
                    r.newValue += "#" + info;
            }
        }
        if (!r.isGen)
            return null;
        else
            return r;
    }

    public void scrubMetadata(String server, int port) throws IOException {
        String member = server + ":" + port;
        Jedis jedis = getRpL1().getResource();
        if (jedis == null)
            throw new IOException(
                    "Invalid RpL1 pool, could not get available connection");

        try {
            Double gd = jedis.zscore("mm.active", member);
            if (gd != null) {
                Long sid = gd.longValue();
                TreeSet<String> sets = getAllSets(jedis);

                for (String set : sets) {
                    System.out.println("-> Begin Scrub SET " + set + " ... ");
                    RedisConnection rc = null;

                    try {
                        rc = rps.getL2(set, false);
                        Jedis j = rc.jedis;
                        if (j != null) {
                            Map<String, String> kvs = j.hgetAll(set);

                            if (kvs != null) {
                                for (Entry<String, String> e : kvs
                                        .entrySet()) {
                                    GenValue gv = __gen_value(e.getValue(), sid);
                                    if (gv != null) {
                                        // ok, do update now
                                        if (gv.newValue != null)
                                            j.hset(set, e.getKey(), gv.newValue);
                                        else
                                            j.hdel(set, e.getKey());
                                        System.out.println("HSET " + set + " "
                                                + e.getKey() + " "
                                                + gv.newValue + " "
                                                + e.getValue());
                                    }
                                }
                                kvs.clear();
                            }
                        }
                    } catch (JedisException e) {
                        System.out.println(set + ": Jedis exception: "
                                + e.getMessage());
                    } catch (Exception e) {
                        System.out.println(set + ": exception: "
                                + e.getMessage());
                    } finally {
                        rps.putL2(rc);
                    }
                }
            } else {
                System.out.println("Find server " + member + " failed.");
            }
        } catch (JedisException e) {
            System.out.println("L1 pool: Jedis Exception: " + e.getMessage());
        } finally {
            getRpL1().putInstance(jedis);
        }
    }

    private final static Comparator<Entry<String, Integer>> comp = new Comparator<Entry<String, Integer>>() {
        @Override
        public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
            return (o2.getValue() - o1.getValue()) > 0 ? 1 : -1;
        }
    };

    private class ObjectSearchThread extends Thread {
        private Entry<Long, String> entry;
        private List<Feature> features;
        private ResultSet rs;
        private byte[] obj;

        public ObjectSearchThread(byte[] obj, List<Feature> features,
                                  Entry<Long, String> entry, ResultSet rs) {
            this.obj = obj;
            this.features = features;
            this.entry = entry;
            this.rs = rs;
        }

        public void run() {
            try {
                String server = entry.getValue();
                SocketHashEntry searchSocket = null;

                searchSocket = socketHash.get(server);
                searchSocket = getSocketHashEntry(searchSocket, server);

                byte[] header = new byte[4];
                header[0] = ActionType.FEATURESEARCH;
                long id = searchSocket.getFreeSocket();
                if (id == -1)
                    throw new IOException(
                            "Could not get free socket for server " + server);
                ResultSet result = null;
                try {
                    searchSocket.map.get(id).dos.write(header);
                    searchSocket.map.get(id).dos.writeInt(features.size());
                    searchSocket.map.get(id).dos.writeInt(obj.length);
                    ObjectOutputStream oos = new ObjectOutputStream(
                            searchSocket.map.get(id).dos);
                    for (Feature f : features) {
                        oos.writeObject(f);
                    }
                    searchSocket.map.get(id).dos.write(obj);
                    searchSocket.map.get(id).dos.flush();

                    result = __handleInput4ResultSet(searchSocket.map.get(id).dis);
                    searchSocket.setFreeSocket(id);
                } catch (Exception e) {
                    e.printStackTrace();
                    // remove this socket do reconnect?
                    searchSocket.delFromSockets(id);
                }
                if (result != null) {
                    rs.addAll(result);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public ResultSet objectSearch(List<Feature> features, byte[] obj,
                                  List<String> specified_servers) throws IOException {
        List<ObjectSearchThread> ost = new ArrayList<ObjectSearchThread>();
        ResultSet rs = new ResultSet(ResultSet.ScoreMode.PROD);
        Map<Long, String> sToSearch = null;

        if (specified_servers != null) {
            long i = 0;
            sToSearch = new HashMap<Long, String>();
            for (String s : specified_servers) {
                sToSearch.put(i++, s);
            }
        } else {
            sToSearch = servers;
        }
        for (Entry<Long, String> entry : sToSearch.entrySet()) {
            ost.add(new ObjectSearchThread(obj, features, entry, rs));
        }
        for (ObjectSearchThread t : ost) {
            t.start();
        }

        for (ObjectSearchThread t : ost) {
            try {
                t.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        return rs;
    }

    public long getCkpt_ts() {
        return ckpt_ts;
    }

    public void setCkpt_ts(long ckpt_ts) {
        this.ckpt_ts = ckpt_ts;
    }

    public long getSs_id() {
        return ss_id;
    }

    public void setSs_id(long ss_id) {
        this.ss_id = ss_id;
    }

    public void setLs_id(long ls_id) {
        this.ls_id = ls_id;
    }
}
