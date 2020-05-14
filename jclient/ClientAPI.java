package mammoth.jclient;

import mammoth.jclient.PhotoClient.SocketHashEntry;
import mammoth.common.MMConf;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.activation.MimetypesFileTypeMap;

public class ClientAPI {
    private PhotoClient pc;
    private AtomicInteger index = new AtomicInteger(0);
    private List<String> keyList = Collections
            .synchronizedList(new ArrayList<>());
    private List<String> keyList4Balance = Collections
            .synchronizedList(new ArrayList<>());
    // 缓存与服务端的tcp连接,服务端名称到连接的映射
    private ConcurrentHashMap<String, SocketHashEntry> socketHash;
    private final Timer timer = new Timer("ActiveMMSRefresher");
    //redis中设置该参数则启动负载均衡，否则使用默认roundRobin策略
    private boolean balance = false;
    private String nameBase;

    public ClientAPI(ClientConf conf) {
        pc = new PhotoClient(conf);
    }

    public ClientAPI() {
        pc = new PhotoClient();
    }

    public enum MMType {
        TEXT, IMAGE, AUDIO, VIDEO, APPLICATION, THUMBNAIL, OTHER, RELOAD, TARGET,
    }

    public static String getMMTypeSymbol(MMType type) {
        switch (type) {
            case TEXT:
                return "t";
            case IMAGE:
                return "i";
            case AUDIO:
                return "a";
            case VIDEO:
                return "v";
            case APPLICATION:
                return "o";
            case THUMBNAIL:
                return "s";
            case TARGET:
                return "z";
            case RELOAD:
                return "x";
            case OTHER:
            default:
                return "";
        }
    }

    /**
     * DO NOT USE this function unless if you know what are you doing.
     *
     * @return PhotoClient
     */
    public PhotoClient getPc() {
        return pc;
    }

    private void updateClientConf(Jedis jedis, ClientConf conf) {
        if (conf == null || !conf.isAutoConf() || jedis == null)
            return;
        try {
            String dupMode = jedis.hget("mm.client.conf", "dupmode");
            if (dupMode != null) {
                if (dupMode.equalsIgnoreCase("dedup")) {
                    conf.setMode(ClientConf.MODE.DEDUP);
                } else if (dupMode.equalsIgnoreCase("nodedup")) {
                    conf.setMode(ClientConf.MODE.NODEDUP);
                }
            }

            String vlen = jedis.hget("mm.client.conf", "vlen");
            if (vlen != null) {
                int vl = Integer.parseInt(vlen);
                if (vl > 0)
                    conf.setVlen(vl);
            }

            String dupNum = jedis.hget("mm.client.conf", "dupnum");
            if (dupNum != null) {
                int dn = Integer.parseInt(dupNum);
                if (dn > 1)
                    conf.setDupNum(dn);
            }
            String sockPerServer =
                    jedis.hget("mm.client.conf", "sockperserver");
            if (sockPerServer != null) {
                int sps = Integer.parseInt(sockPerServer);
                if (sps >= 1)
                    conf.setSockPerServer(sps);
            }
            String dupinfo = jedis.hget("mm.client.conf", "dupinfo");
            if (dupinfo != null) {
                int di = Integer.parseInt(dupinfo);
                if (di > 0)
                    conf.setLogDupInfo(true);
                else
                    conf.setLogDupInfo(false);
            }
            String mgetTimeout = jedis.hget("mm.client.conf", "mgetto");
            if (mgetTimeout != null) {
                int to = Integer.parseInt(mgetTimeout);
                if (to > 0)
                    conf.setMgetTimeout(to * 1000);
            }
            
            String DataSynchronization = jedis.hget("mm.client.conf", "ds");
            if (DataSynchronization != null) {
                int ds = Integer.parseInt(DataSynchronization);
                if (ds > 0)
                    conf.setDataSync(true);
                else
                    conf.setDataSync(false);
            }
            System.out.println("Auto conf client with: dupMode="
                    + conf.getMode() + ", dupNum=" + conf.getDupNum()
                    + ", logDupInfo=" + conf.isLogDupInfo()
                    + ", sockPerServer=" + conf.getSockPerServer()
                    + ", mgetTimeout=" + conf.getMgetTimeout()
                    + ", blockLen=" + conf.getBlen());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private int init_by_sentinel(ClientConf conf, String urls) throws Exception {
        if (conf.getRedisMode() != MMConf.RedisMode.SENTINEL) {
            return -1;
        }
        // iterate the sentinel set, get master IP:port, save to sentinel set
        if (conf.getSentinels() == null) {
            if (urls == null) {
                throw new Exception("Invalid URL(null) or sentinels.");
            }
            HashSet<String> sens = new HashSet<String>();
            String[] s = urls.split(";");
            for (int i = 0; i < s.length; i++) {
                sens.add(s[i]);
            }
            conf.setSentinels(sens);
        }
        pc.init();
        Jedis jedis = pc.getRpL1().getResource();
        try {
            if (jedis != null)
                updateClientConf(jedis, conf);
        } finally {
            pc.getRpL1().putInstance(jedis);
        }

        return 0;
    }

    private int init_by_standalone(ClientConf conf, String urls)
            throws Exception {
        if (conf.getRedisMode() != MMConf.RedisMode.STANDALONE) {
            return -1;
        }
        // get IP:port, save it to HaP
        if (urls == null) {
            throw new Exception("Invalid URL: null");
        }
        String[] s = urls.split(":");
        if (s != null && s.length == 2) {
            try {
                HostAndPort hap = new HostAndPort(s[0], Integer.parseInt(s[1]));
                conf.setHap(hap);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        pc.init();
        Jedis jedis = pc.getRpL1().getResource();
        try {
            if (jedis != null)
                updateClientConf(jedis, conf);
        } finally {
            pc.getRpL1().putInstance(jedis);
        }
        return 0;
    }

    private boolean refreshActiveMMS(boolean isInit) {
        // refresh active secondary server and ckpt.ts now
        getActiveSecondaryServer();
        if (isInit) {
            return getActiveMMS();
        } else {
                List<String> active = pc.getActiveMMSByHB();
                Set<String> activeMMS = new TreeSet<String>();

                // BUG-XXX: getActiveMMSByHB() return the DNSed server name, it
                // might be different
                // with saved server name. Thus, if a server changes its ip
                // address, we can't
                // find it even we put it into socketHash.
                // Possibly, we get server_name:server_ip pair, and try to find
                // by name and ip.
                // If server_name in servers, check up
                // socketHash.get(server_name) {free it?}
                // and check up socketHash.get(server_ip), finally update
                // server_ip to servers?
                // else if server_ip in servers, it is ok.
                // else register ourself to servers?
                if (active.size() > 0) {
                    for (String a : active) {
                        String[] c = a.split(":");
                        if (c.length == 2) {
                            if (socketHash.get(a) == null) {
                                // new MMS?
                                Socket sock = new Socket();
                                SocketHashEntry she = new SocketHashEntry(c[0],
                                        Integer.parseInt(c[1]), pc.getConf()
                                        .getSockPerServer());
                                try {
                                    sock.setTcpNoDelay(true);
                                    sock.connect(new InetSocketAddress(c[0],
                                            Integer.parseInt(c[1])));
                                    she.addToSockets(
                                            sock,
                                            new DataInputStream(sock
                                                    .getInputStream()),
                                            new DataOutputStream(sock
                                                    .getOutputStream()));
                                    //认证信息
                                    byte[] bytes = new byte[2];
                                    bytes[0] = (byte) ClientConf.userName.length();
                                    bytes[1] = (byte) ClientConf.passWord.length();
                                    sock.getOutputStream().write(bytes);
                                    sock.getOutputStream().write((ClientConf.userName + ClientConf.passWord).getBytes());
                                    sock.getOutputStream().flush();
                                    DataInputStream dis = new DataInputStream(sock.getInputStream());
                                    int num = dis.readInt();
                                    if (num == -1) {
                                        System.out.println("认证失败，客户端与服务端版本不一致！！");
                                        sock.close();
                                    }
                                    else System.out.println("认证成功！");
                                    if (socketHash.putIfAbsent(a, she) != null) {
                                        she.clear();
                                    }
                                } catch (SocketException e) {
                                    e.printStackTrace();
                                    continue;
                                } catch (NumberFormatException e) {
                                    e.printStackTrace();
                                    continue;
                                } catch (IOException e) {
                                    e.printStackTrace();
                                    continue;
                                }
                            }
                            activeMMS.add(a);
                        }
                    }
                    if(balance){
                        synchronized (keyList4Balance){
                            keyList4Balance.clear();
                            initKeyList4Balance();
                        }
                    }
                    synchronized (keyList) {
                        keyList.clear();
                        keyList.addAll(activeMMS);
                        keyList.retainAll(socketHash.keySet());
                        //初始化keyList4Balance
                    }
                } else {
                    keyList.clear();
                }
                if (pc.getConf().isPrintServerRefresh())
                    System.out.println("Refresh active servers: " + keyList);

        }
        return true;
    }

    private void getActiveSecondaryServer() {
        Jedis jedis = pc.getRpL1().getResource();
        if (jedis == null)
            return;
        String ss = null, ckpt = null;
        String ls = null;

        try {
            ss = jedis.get("mm.ss.id");
            ls = jedis.get("mm.ls.id");
            ckpt = jedis.get("mm.ckpt.ts");
            balance = jedis.get("mm.client.balance") == null? false:true;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String s = df.format(new Date());
//            System.out.println(s + " -- balance = " + balance + "; ss = " + ss + "; ls = " + ls );
        } catch (Exception e) {
            System.out.println("getActiveSecondaryServer exception: "
                    + e.getMessage());
        } finally {
            pc.getRpL1().putInstance(jedis);
        }
        try {
            if (ss != null)
                pc.setSs_id(Long.parseLong(ss));
            if (ls != null)
                pc.setLs_id(Long.parseLong(ls));
        } catch (Exception e) {
            System.out.println("Convert secondary server id '" + ss
                    + "' to LONG failed.");
        }
        try {
            if (ckpt != null)
                pc.setCkpt_ts(Long.parseLong(ckpt));
        } catch (Exception e) {
            System.out.println("Convert checkpoint ts '" + ckpt
                    + "' to LONG failed.");
        }
    }

    private boolean getActiveMMS() {
        Jedis jedis = pc.getRpL1().getResource();
        if (jedis == null)
            return false;

        try {
            Set<Tuple> servers = jedis.zrangeWithScores("mm.active", 0, -1);
            List<String>  active = pc.getActiveMMSByHB();
            balance = jedis.get("mm.balance") == null? false:true;
            if (servers != null && servers.size() > 0) {
                for (Tuple t : servers) {
                    pc.addToServers((long) t.getScore(), t.getElement());
                }
            }
            for (String a : active) {
                String[] c = a.split(":");
                if (c.length == 2) {
                    if (socketHash.get(a) == null) {
                        // new MMS?
                        Socket sock = new Socket();
                        SocketHashEntry she = new SocketHashEntry(c[0],
                                Integer.parseInt(c[1]), pc.getConf()
                                .getSockPerServer());
                        try {
                            sock.setTcpNoDelay(true);
                            sock.connect(new InetSocketAddress(c[0],
                                    Integer.parseInt(c[1])));
                            she.addToSockets(
                                    sock,
                                    new DataInputStream(sock
                                            .getInputStream()),
                                    new DataOutputStream(sock
                                            .getOutputStream()));
                            //认证信息
                            byte[] bytes = new byte[2];
                            bytes[0] = (byte) ClientConf.userName.length();
                            bytes[1] = (byte) ClientConf.passWord.length();
                            sock.getOutputStream().write(bytes);
                            sock.getOutputStream().write((ClientConf.userName + ClientConf.passWord).getBytes());
                            sock.getOutputStream().flush();
                            DataInputStream dis = new DataInputStream(sock.getInputStream());
                            int num = dis.readInt();
                            if (num == -1) {
                                System.out.println("认证失败，客户端与服务端版本不一致！！");
                                sock.close();
                            }
                            else System.out.println("认证成功！");
                            if (socketHash.putIfAbsent(a, she) != null) {
                                she.clear();
                            }
                        } catch (SocketException e) {
                            e.printStackTrace();
                            continue;
                        } catch (NumberFormatException e) {
                            e.printStackTrace();
                            continue;
                        } catch (IOException e) {
                            e.printStackTrace();
                            continue;
                        }
                    }
                }
            }
            if(balance){
                synchronized (keyList4Balance){
                    keyList4Balance.clear();
                    initKeyList4Balance();
                }
            }
            synchronized (keyList) {
                keyList.addAll(active);
                keyList.retainAll(socketHash.keySet());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            pc.getRpL1().putInstance(jedis);
        }

        return true;
    }

    private class MMCTimerTask extends TimerTask {
        @Override
        public void run() {
            try {
                refreshActiveMMS(false);
            } catch (Exception e) {
		e.printStackTrace();
                System.out.println("[ERROR] refresh active MMS failed: "
                        + e.getMessage() + ".\n" + e.getCause());
            }
        }
    }

    /**
     * 连接服务器,进行必要初始化,并与redis服务器建立连接 如果初始化本对象时传入了conf，则使用conf中的redis地址，否则使用参数url
     * It is not thread-safe!
     *
     * @param urls MM STANDARD PROTOCOL: STL:// or STA://
     * @return
     * @throws Exception
     */
    public int init(String urls) throws Exception {
        if (urls == null) {
            throw new Exception("The url can not be null.");
        }
        if (pc.getConf() == null) {
            pc.setConf(new ClientConf());
        }
        if (urls.startsWith("STL://")) {
            urls = urls.substring(6);
            pc.getConf().setRedisMode(ClientConf.RedisMode.SENTINEL);
        } else if (urls.startsWith("STA://")) {
            urls = urls.substring(6);
            pc.getConf().setRedisMode(ClientConf.RedisMode.STANDALONE);
        }
        switch (pc.getConf().getRedisMode()) {
            case SENTINEL:
                init_by_sentinel(pc.getConf(), urls);
                break;
            case STANDALONE:
                init_by_standalone(pc.getConf(), urls);
                break;
            case CLUSTER:
                System.out.println("MMS do NOT support CLUSTER mode now, "
                        + "use STL/STA instead.");
                break;
            default:
                break;
        }

        socketHash = new ConcurrentHashMap<>();
        // 从redis上获取所有的服务器地址
        refreshActiveMMS(true);
        System.out.println("Got active server size=" + keyList.size());
        pc.setSocketHash(socketHash);
        timer.schedule(new MMCTimerTask(), 500, pc.getConf()
                .getServerRefreshInterval());
        return 0;
    }


    /**
     * 从redis查找每个server剩余磁盘空间
     * @return
     */
    private Map<String, Long> getSpaceRate(){
        Map<Long, String> servers = pc.getServers();
        Jedis jedis = null;
        Map<Long, Long> spaceServerId;//serverId space
        Map<String, Long> spaceServer = new HashMap<>();//ip:port space
        try{
            jedis = pc.getRpL1().getResource();
            spaceServerId = new HashMap<>();
            Map<String, String> spaceDisk = jedis.hgetAll("mm.space");
            long serverId;
            for(Map.Entry<String, String> entry : spaceDisk.entrySet()){
                serverId = Long.parseLong(entry.getKey().split("\\|")[0]);
                if(entry.getKey().endsWith("F"))
                    spaceServerId.put(serverId,
                            (spaceServerId.get(serverId) == null ? 0L :spaceServerId.get(serverId)) + Long.parseLong(entry.getValue()));
            }
            for(Map.Entry<Long, Long> entry : spaceServerId.entrySet()){
                spaceServer.put(servers.get(entry.getKey()), entry.getValue());
            }
        }catch (JedisException e){
            e.printStackTrace();
        }finally{
            pc.getRpL1().putInstance(jedis);
        }
        return spaceServer;
    }

    /**
     * double 四舍五入成Int
     * @param dou
     * @return
     */
    public static int DoubleFormatInt(Double dou){
        DecimalFormat df = new DecimalFormat("######0"); //四色五入转换成整数
        return Integer.parseInt(df.format(dou));
    }

    /**
     * 负载均衡算法生成新的keyList
     * @param numServer
     */
    private void initKeyList4BalanceMidStep(Map<String, Integer> numServer){
        Map<String, Integer> map = new HashMap<>();
        map.putAll(numServer);

        while(!numServer.isEmpty()){
            for (Map.Entry<String, Integer> entry : map.entrySet()){
                if ( entry.getValue() == 1 || !keyList.contains(entry.getKey())) {
                    numServer.remove(entry.getKey());
                } else {
                    keyList4Balance.add(entry.getKey());
                    numServer.put(entry.getKey(), entry.getValue() - 1);
                }
            }
        }
        int listLength = keyList4Balance.size();
        String lastOne = keyList4Balance.get(keyList4Balance.size() - 1);
        String lastButOne = keyList4Balance.get(keyList4Balance.size() - 2);
        String firstOne = keyList4Balance.get(0);
        if(firstOne.equals(lastButOne)){
            keyList4Balance.set(listLength - 2, lastOne);
            keyList4Balance.set(listLength - 1, lastButOne);
        }else if (lastButOne.equals(lastOne)){
            keyList4Balance.set(0, lastOne);
            keyList4Balance.set(listLength - 1, firstOne);
        }
    }

    /**
     *
     */
    private void initKeyList4Balance(){
        keyList4Balance.addAll(keyList);
        Map<String, Long> spaceServer = getSpaceRate();//serverId space
        Map<String, Integer> numServer = new HashMap<>();
        long minSpace = Long.MAX_VALUE;
        for (Map.Entry<String, Long> entry : spaceServer.entrySet()){
            if(entry.getValue() < minSpace)
                minSpace = entry.getValue();
        }
        for (Map.Entry<String, Long> entry : spaceServer.entrySet()){
            numServer.put(entry.getKey(), DoubleFormatInt((double)entry.getValue()/(double)minSpace));
        }
        initKeyList4BalanceMidStep(numServer);
    }

    /**
     *
     * @param targets
     * @param keys
     * @param content
     * @param nodedup
     * @param fn
     * @return
     * @throws Exception
     */
    private String __put(Set<String> targets, String[] keys, byte[] content,
                         boolean nodedup, String fn) throws Exception {
        Random rand = new Random();
        String r = null;
        Set<String> saved = new TreeSet<>();
        HashMap<String, Long> failed = new HashMap();
        int targetnr = targets.size();
        do {
            for (String server : targets) {
                SocketHashEntry she = socketHash.get(server);
                if (she.probSelected()) {
                    // BUG-XXX: we have to check if we can recover from this
                    // exception,
                    // then try our best to survive.
                    try {
                        r = pc.syncStorePhoto(keys[0], keys[1], content, she,
                                nodedup, content.length, 0, fn);
                        if ('#' == r.charAt(1))
                            r = r.substring(2);
                        nodedup = r.split("#").length < pc.getConf().getDupNum();
                        saved.add(server);
                    } catch (Exception e) {
                        e.printStackTrace();
                        if (failed.containsKey(server)) {
                            failed.put(server, failed.get(server) + 1);
                        } else
                            failed.put(server, 1L);
                            System.out.println("[PUT] " + keys[0] + "@" + keys[1]
                                + " to " + server + " failed: ("
                                + e.getMessage() + ") for "
                                + failed.get(server) + " times.");
                    }
                } else {
                    //this means target server has no current usable connection.
                }
            }
            if (saved.size() < targetnr) {
                List<String> remains = new ArrayList<>(keyList);
                remains.removeAll(saved);
                for (Map.Entry<String, Long> e : failed.entrySet()) {
                    if (e.getValue() > 2) {
                        remains.remove(e.getKey());
                    }
                }
                if (remains.size() == 0) {
                    break;
                }
                targets.clear();
                for (int i = saved.size(); i < targetnr; i++) {
                    targets.add(remains.get(rand.nextInt(remains.size())));
                }
            } else
                break;
        } while (true);
        if (saved.size() == 0) {
            throw new Exception("Error in saving Key: " + keys[0] + "@"
                    + keys[1]);
        }
        return r;
    }


    /**
     * 同步写, It is thread-safe!
     *
     * @param key
     * @param content
     * @param fn
     * @return info to locate the file
     * @throws Exception
     */
    private String put(String key, byte[] content, String fn) throws Exception {
        if (key == null || keyList.size() == 0) {
            throw new Exception("Key can not be null or no active MMServer ("
                    + keyList.size() + ").");
        }
        String[] keys = key.split("@");
        if (keys.length != 2)
            throw new Exception("Wrong format of key: " + key);
        boolean nodedup = false;
        int dupnum = Math.min(keyList.size(), pc.getConf().getDupNum());

        // roundrobin select dupnum servers from keyList, if error in put,
        // random select in remain servers
        Set<String> targets = new TreeSet<>();
        int idx = index.getAndIncrement();
        if (idx < 0) {
            index.compareAndSet(idx, 0);
            idx = index.get();
        }
        if(balance){
            for (int i = 0; i < dupnum; i++){
                targets.add(keyList4Balance.get((idx + i) % keyList4Balance.size()));
            }
        }else{
            for (int i = 0; i < dupnum; i++)
                targets.add(keyList.get((idx + i) % keyList.size()));
        }
        return __put(targets, keys, content, nodedup, fn);
    }

    /**
     * 同步写,对外提供的接口 It is thread-safe!
     * @param key
     * @param content
     * @return
     * @throws Exception
     */
    public String put(String key, byte[] content) throws Exception {
        return put(key, content, null);
    }

    /**
     *
     * @param type
     * @param time
     * @param md5
     * @param content
     * @return
     * @throws Exception
     */
    public String put(KeyFactory.KeyType type, long time, String md5, byte[] content) throws Exception {
        String key = KeyFactory.getInstance(type, md5, time);
        return put(key, content);
    }

    /**
     *
     * @param type
     * @param md5
     * @param content
     * @return
     * @throws Exception
     */
    public String put(KeyFactory.KeyType type, String md5, byte[] content) throws Exception {
        String key = KeyFactory.getInstance(type, md5, Instant.now().getEpochSecond());
        return put(key, content);
    }

    /**
     *
     * @param type
     * @param content
     * @return
     * @throws Exception
     */
    public String put(KeyFactory.KeyType type, byte[] content) throws Exception {
        String key = KeyFactory.getInstance(type, content);
        return put(key, content);
    }

    /**
     * 
     * @param localFilePath
     * @return key
     * @throws Exception
     */
    public String uploadFile(String localFilePath) throws Exception {
    	File file = new File(localFilePath);
    	return uploadFile(file);
    }
    /**
     * 
     * @param file
     * @return key
     * @throws Exception
     */
    public String uploadFile(File file) throws Exception {
        //TODO:key need to be create
    	String key = "";
        String info = uploadFile(file, key);
        if (info == null)
        	key = null;
        return key;
    }
    /**
     * 
     * @param file
     * @param key
     * @return info
     * @throws Exception
     */
    public String uploadFile(File file, String key) throws Exception {
    	//filename
        String fn = file.getName();
        RandomAccessFile randomFile = null;
        String info;
        try {
            randomFile = new RandomAccessFile(file, "r");
            long fileLength = randomFile.length();
            byte[] bytes = new byte[(int) fileLength];
            randomFile.seek(0);
            randomFile.read(bytes);
            info = put(key, bytes, fn);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        } finally {
            if (randomFile != null) {
                try {
                    randomFile.close();
                } catch (IOException e) {
                	e.printStackTrace();
                	return null;
                }
            }
        }
        return info;
    }
    /**
     * 异步写,对外提供的接口（暂缓使用）
     *
     * @param key
     * @param content
     * @return
     */
    public void iPut(String key, byte[] content) throws IOException, Exception {
        //TODO:
        /*
         * if(key == null) throw new Exception("key can not be null."); String[]
		 * keys = key.split("@"); if(keys.length != 2) throw new
		 * Exception("wrong format of key:"+key); for (int i = 0; i <
		 * pc.getConf().getDupNum(); i++) { // Socket sock =
		 * socketHash.get(keyList.get((index + i) % keyList.size())); //
		 * pc.asyncStorePhoto(keys[0], keys[1], content, sock); } index++;
		 * if(index >= socketHash.size()){ index = 0; }
		 */
    }

    /**
     * 批量同步写,对外提供的接口（暂缓使用）
     *
     * @param set
     * @param md5s
     * @param content
     * @return
     */
    public String[] mPut(String set, String[] md5s, byte[][] content)
            throws Exception {
        if (set == null || md5s.length == 0 || content.length == 0) {
            throw new Exception("set or md5s or contents can not be null.");
        } else if (md5s.length != content.length)
            throw new Exception("arguments length mismatch.");
        String[] r = null;
        int idx = index.getAndIncrement();

        if (idx < 0) {
            index.compareAndSet(idx, 0);
            idx = index.get();
        }
        for (int i = 0; i < pc.getConf().getDupNum(); i++) {
            SocketHashEntry she = socketHash.get(keyList.get((idx + i)
                    % keyList.size()));
            r = pc.mPut(set, md5s, content, she);
        }

        return r;
    }

    /**
     * 批量异步写，对外提供的接口（暂缓使用）
     *
     * @param keys redis中的键以set开头+#+md5的字符串形成key
     * @return 图片内容, 如果图片不存在则返回长度为0的byte数组
     */
    public void imPut(String[] keys, byte[][] contents) throws Exception {
        //TODO:
		/*
		 * if(keys.length != contents.length){ throw new
		 * Exception("keys's length is not the same as contents'slength."); }
		 * for(int i = 0;i<keys.length;i++){ iPut(keys[i],contents[i]); }
		 */
    }

    /**
     * 根据info从系统中读取对象内容，重新写入到MMServer上仅当info中活动的MMServer个数小于dupnum时
     * <p>
     * (should only be used by system tools)
     *
     * @param key
     * @param info
     * @return
     * @throws IOException
     * @throws Exception
     */
    public String xput(String key, String info, String fn) throws IOException, Exception {
        if (key == null || keyList.size() == 0) {
            throw new Exception("Key can not be null or no active MMServer (" +
                    keyList.size() + ").");
        }
        String[] keys = key.split("@");
        if (keys.length != 2)
            throw new Exception("Wrong format of key: " + key);

        boolean nodedup = true;
        int dupnum = Math.min(keyList.size(), pc.getConf().getDupNum());

//        if (content == null) {
//            throw new IOException("Try to get content of KEY: " + key + " failed.");
//        }
        // round-robin select dupnum servers from keyList, if error in put,
        // random select in remain servers
        TreeSet<String> targets = new TreeSet<String>();
        Set<String> active = new TreeSet<String>();
        targets.addAll(keyList);

        String[] infos = info.split("#");
        for (String s : infos) {
            long sid = Long.parseLong(s.split("@")[2]);
            String server = this.getPc().getServers().get(sid);
            if (server != null) {
                targets.remove(server);
                active.add(server);
            }
        }
        if (active.size() >= dupnum)
            return info;
        if (targets.size() == 0)
            throw new IOException("No more copy can be made: " + key + " --> " + info);
        else if (targets.size() > (dupnum - active.size())) {
            Random rand = new Random();
            String[] tArray = targets.toArray(new String[0]);
            targets.clear();
            for (int i = 0; i < dupnum - active.size(); i++) {
                targets.add(tArray[rand.nextInt(tArray.length)]);
            }
        }
        byte[] content = this.get(info);
        info = __put(targets, keys, content, nodedup, fn);
        return info;
    }

    /**
     * 获取文件信息
     *
     * @param key
     * @return
     * @throws Exception
     */
    public FileInfo getFile(String key) throws Exception {
        if (key == null)
            throw new Exception("key can not be null.");
        return pc.getFileInfo(key);
    }

    /**
     *
     * @param key
     * @param offset
     * @param length
     * @return
     * @throws Exception
     */
    private byte[] get(String key, long offset, int length) throws Exception {
        if (key == null)
            throw new Exception("key can not be null.");
        String[] keys = key.split("@|#");
        /*//TODO:1
        if (keys.length == 2)
            return pc.getPhoto(keys[0], keys[1], offset, length);
        //TODO:2 useless
        else if (keys.length == pc.getConf().getVlen())
            return pc.searchByInfo(key, keys, offset, length);
        //TODO:3 useless
        else if (keys.length % pc.getConf().getVlen() == 0) // 如果是拼接的元信息，分割后长度是7的倍数
            return pc.searchPhoto(key, offset, length);
        else
            throw new Exception("wrong format of key:" + key);*/
        return null;
    }

    /**
     * 同步取，对外提供的接口 It is thread-safe
     *
     * @param key 或者是set@md5,或者是文件元信息，可以是拼接后的
     * @return 图片内容, 如果图片不存在则返回长度为0的byte数组
     * @throws IOException
     * @throws Exception
     */
    public byte[] get(String key) throws IOException, Exception {
        if (key == null)
            throw new Exception("key can not be null.");
        String[] keys = key.split("@|#");
        if (keys.length == 2)
            return pc.getPhoto(keys[0], keys[1]);
        else if (keys.length == pc.getConf().getVlen())
            return pc.searchByInfo(key, keys);
        else if (keys.length % pc.getConf().getVlen() == 0) // 如果是拼接的元信息，分割后长度是8的倍数
            return pc.searchPhoto(key);
        else
            throw new Exception("wrong format of key:" + key);
    }
    /**
     * 批量读取某个集合的所有key It is thread-safe
     *
     * @param type
     * @param begin_time
     * @return
     * @throws IOException
     * @throws Exception
     */
    public List<String> getkeys(String type, long begin_time)
            throws IOException, Exception {
        String prefix;

        if (type == null)
            throw new Exception("type can not be null");
        if (type.equalsIgnoreCase("image")) {
            prefix = getMMTypeSymbol(MMType.IMAGE);
        } else if (type.equalsIgnoreCase("thumbnail")) {
            prefix = getMMTypeSymbol(MMType.THUMBNAIL);
        } else if (type.equalsIgnoreCase("text")) {
            prefix = getMMTypeSymbol(MMType.TEXT);
        } else if (type.equalsIgnoreCase("audio")) {
            prefix = getMMTypeSymbol(MMType.AUDIO);
        } else if (type.equalsIgnoreCase("video")) {
            prefix = getMMTypeSymbol(MMType.VIDEO);
        } else if (type.equalsIgnoreCase("application")) {
            prefix = getMMTypeSymbol(MMType.APPLICATION);
        } else if (type.equalsIgnoreCase("other")) {
            prefix = getMMTypeSymbol(MMType.OTHER);
        } else {
            throw new Exception("type '" + type + "' is invalid.");
        }
        // query on redis
        TreeSet<Long> tranges = pc.getSets(prefix);
        try {
            long setTs = tranges.tailSet(begin_time).first();
            return pc.getSetElements(prefix + setTs);
        } catch (NoSuchElementException e) {
            throw new Exception("Can not find any keys larger or equal to "
                    + begin_time);
        }
    }

    /**
     * 批量获取keys对应的所有多媒体对象内容 it is thread safe.
     *
     * @param keys
     * @param cookies
     * @return
     * @throws IOException
     * @throws Exception
     */
    public List<byte[]> mget(List<String> keys, Map<String, String> cookies)
            throws IOException, Exception {
        if (keys == null || cookies == null)
            throw new Exception("keys or cookies list can not be null.");
        if (keys.size() > 0) {
            String key = keys.get(keys.size() - 1);
            if (key != null) {
                long ts = -1;
                try {
                    ts = Long.parseLong(key.split("@")[0].substring(1));
                } catch (Exception ignored) {
                }
                cookies.put("ts", Long.toString(ts));
            }
        }
        return pc.mget(keys, cookies);
    }

    /**
     * 同步删，对外提供的接口 It is thread-safe
     *
     * @param key set@md5
     * @throws Exception
     */
    public void deleteFile(String key) throws Exception {
        if (key == null)
            throw new Exception("key can not be null.");
        String[] keys = key.split("@");
        if (keys.length == 2)
            pc.deletePhoto(keys[0], keys[1]);
        else
            throw new Exception("wrong format of key:" + key);
    }
    
    /**
     * 退出多媒体客户端，释放内部资源
     */
    public void quit() {
        pc.close();
        timer.cancel();
    }
}
