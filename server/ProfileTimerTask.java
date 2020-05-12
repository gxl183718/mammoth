package mammoth.server;

import mammoth.common.LogTool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisException;

import java.io.*;
import java.net.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProfileTimerTask extends TimerTask {
    private static LogTool logTool = new LogTool();
    private ServerConf conf;
    public int period;
    private long lastWns = 0;
    private long lastRns = 0;
    private double lastWn = 0;
    private double lastRn = 0;
    private long lastDl = 0;
    private long lastDnr = 0;
    private long lastTs = System.currentTimeMillis();
    private long lastRecycleTs = System.currentTimeMillis();
    private String profileDir = "log/";
    private String hbkey;
    private DatagramSocket client = null;
    private PubSubThread pst = null;

    public class PubSubThread extends Thread {
        private MMSPubSub mps;

        public PubSubThread(MMSPubSub mps) {
            this.mps = mps;
        }

        public void run() {
            Jedis jedis = StorePhoto.getRpL1(conf).getResource();
            if (jedis != null) {
                try {
                    jedis.psubscribe(mps, "mm.info.*");
                } finally {
                    StorePhoto.getRpL1(conf).putInstance(jedis);
                }
            }
        }
    }

    public ProfileTimerTask(ServerConf conf, int period) throws JedisException {
        super();
        this.conf = conf;
        File dir = new File(profileDir);
        if (!dir.exists())
            dir.mkdirs();

        // 向redis的数据库1中插入心跳信息
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();
        if (jedis == null)
            throw new JedisException("Get default jedis instance failed.");
        try {
//            hbkey = "mm.hb." + conf.getNodeName() + ":" + conf.getServerPort();
            hbkey = "mm.hb." + conf.getOutsideIP() + ":" + conf.getServerPort();
            Pipeline pi = jedis.pipelined();
            pi.set(hbkey, "1");
            pi.expire(hbkey, period + 5);
            pi.sync();

            // update mm.dns for IP info
            if (conf.getOutsideIP() != null) {
//                jedis.hset("mm.dns", conf.getNodeName() + ":" + conf.getServerPort(),
//                        conf.getOutsideIP() + ":" + conf.getServerPort());
//                // BUG-XXX: add HTTP port dns service
//                jedis.hset("mm.dns", conf.getNodeName() + ":" + conf.getHttpPort(),
//                        conf.getOutsideIP() + ":" + conf.getHttpPort());
//                logger.info("Update mm.dns for " + conf.getNodeName() + " -> " +
//                        conf.getOutsideIP());
            }

            //注册服务， 每个机器名对应一个唯一的分数，作为serverId
            // determine the ID of ourself, register ourself
//            String self = conf.getNodeName() + ":" + conf.getServerPort();
            String self = conf.getOutsideIP() + ":" + conf.getServerPort();
            Long sid;
            if (jedis.zrank("mm.active", self) == null) {
                sid = jedis.incr("mm.next.serverid");
                // FIXME: if two server start with the same port, fail!m'm
                jedis.zadd("mm.active", sid, self);
            }
            // reget the sid
            sid = jedis.zscore("mm.active", self).longValue();
            ServerConf.serverId = sid;
            logTool.info("Got ServerID " + sid + " for Server " + self);
            // use the same serverID to register in mm.active.http
//            self = conf.getNodeName() + ":" + conf.getHttpPort();
            self = conf.getOutsideIP() + ":" + conf.getHttpPort();
            jedis.zadd("mm.active.http", sid, self);
            logTool.info("Register HTTP server " + self + " done.");

            Set<Tuple> active = jedis.zrangeWithScores("mm.active.http", 0, -1);
            if (active != null && active.size() > 0) {
                for (Tuple t : active) {
                    ServerConf.servers.put((long) t.getScore(), t.getElement());
                    logTool.info("Got HTTP Server " + (long) t.getScore() + " " +
                            t.getElement());
                }
                ServerConf.activeServers = ServerConf.servers;
            }

            // set SS_ID
            if (conf.isSSMaster()) {
                jedis.set("mm.ss.id", "" + ServerConf.serverId);
                logTool.info("Register SS ID to " + ServerConf.serverId);

                //TODO:c语言客服端发布的消息在这里订阅
                // setup pub/sub channel
                pst = new PubSubThread(new MMSPubSub(conf));
                pst.start();
                logTool.info("Setup MM stat info to channel mm.s.info");
            }

            if (conf.isLeSlave()) {
                jedis.set("mm.ls.id", "" + ServerConf.serverId);
                logTool.info("Register LevelDB slave ID to " + ServerConf.serverId);
            }
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
        this.period = period;
        if (conf.getSysInfoServerName() != null && conf.getSysInfoServerPort() != -1) {
            try {
                client = new DatagramSocket();
            } catch (SocketException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void run() {
        try {
            long cur = System.currentTimeMillis();
            double wn = ServerProfile.writtenBytes.longValue() / 1024.0;
            double rn = ServerProfile.readBytes.longValue() / 1024.0;
            double wbw = (wn - lastWn) / ((cur - lastTs) / 1000.0);
            double rbw = (rn - lastRn) / ((cur - lastTs) / 1000.0);
            long dnr = ServerProfile.readN.longValue();
            long dl = ServerProfile.readDelay.longValue();
	    long ws = ServerProfile.writeN.longValue();
	    long rs = ServerProfile.readN.longValue();
            long wns = ws - lastWns;
            long rns = rs - lastRns;
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String s = df.format(new Date());
            String info = s + " avg write bandwidth " + (String.format("%.4f", wbw)) + " KB/s";
            String line = (System.currentTimeMillis() / 1000) + "," + wbw + "," + rbw + ",";

            if ((dl - lastDl) == 0) {
                info += ", no read requests";
                line += "0,";
            } else {
                info += ", avg read latency " +
                        (String.format("%.4f", (double) (dl - lastDl) / (dnr - lastDnr))) + " ms";
                line += (double) (dl - lastDl) / (dnr - lastDnr) + ",";
            }
            if (cur - lastRecycleTs >= 5 * 60 * 1000) {
                info += ", recycle Write " + StorePhoto.recycleContextHash() +
                        ", Read " + StorePhoto.recycleRafHash();
                lastRecycleTs = cur;
            }

//            logger.info(info);
            logTool.mammoth(rns, wns, rbw, wbw);
            // append profiles to log file. Total format is:
            // TS, wbw, rbw, latency,
            // writtenBytes, readBytes, readDelay, readN, readErr, writeN, writeErr,
            line += ServerProfile.writtenBytes.get() + ",";
            line += ServerProfile.readBytes.get() + ",";
            line += ServerProfile.readDelay.get() + ",";
            line += ServerProfile.readN.get() + ",";
            line += ServerProfile.readErr.get() + ",";
            line += ServerProfile.writeN.get() + ",";
            line += ServerProfile.writeErr.get();
            line += "\n";

            lastWn = wn;
            lastRn = rn;
            lastDnr = dnr;
            lastDl = dl;
            lastTs = cur;
            lastRns = rs;
            lastWns = ws;

            // 1.server的心跳信息
            // 2.统计总带宽(gxl)
            Jedis jedis = null;
            try {
                jedis = StorePhoto.getRpL1(conf).getResource();
                if (jedis == null)
                    info += ", redis down?";
                else {
                    Pipeline pi = jedis.pipelined();
                    pi.set(hbkey, "1");
                    pi.expire(hbkey, period + 5);
                    pi.sync();
//TODO:
                    // update server list
                    Set<Tuple> active = jedis.zrangeWithScores("mm.active.http", 0, -1);
                    if (active != null && active.size() > 0) {
                        for (Tuple t : active) {
                            ServerConf.servers.put((long) t.getScore(), t.getElement());
                        }
                    }
                    //update by mm.hb.*
//                    getActiveMMSByHB(conf);
                    // update ss master
                    String ss = jedis.get("mm.ss.id");
//                    String ls = jedis.get("mm.ls.id");
                    try {
                        if (ss != null)
                            ServerConf.setSs_id(Long.parseLong(ss));
//                        if (ls != null)
//                            ServerConf.setLsId(Long.parseLong(ls));
                    } catch (Exception e) {
                    }
                    //记录redis删除到某一个set时间段了
//                    String ckpt = jedis.get("mm.ckpt.ts");
//                    try {
//                        if (ckpt != null)
//                            ServerConf.setCkpt_ts(Long.parseLong(ckpt));
//                    } catch (Exception e) {
//                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                StorePhoto.getRpL1(conf).putInstance(jedis);
            }

            //TODO:没有发现哪里有这个服务
            // Report current state to SysInfoStat Server
//            if (line.length() > 0 && client != null) {
//                String toSend = conf.getNodeName() + "," + conf.getServerPort() + "," + line;
//                byte[] sendBuf = toSend.getBytes();
//                DatagramPacket sendPacket;
//                try {
//                    sendPacket = new DatagramPacket(sendBuf, sendBuf.length,
//                            InetAddress.getByName(conf.getSysInfoServerName()),
//                            conf.getSysInfoServerPort());
//                    client.send(sendPacket);
//                } catch (UnknownHostException e) {
//                    e.printStackTrace();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }

            // 把统计信息写入文件,每一天的信息放在一个文件里
//            String profileName = conf.getNodeName() + "." + conf.getServerPort() + "." +
            String profileName = conf.getOutsideIP() + "." + conf.getServerPort() + "." +
                    s.substring(0, 10) + ".log";
            FileWriter fw = null;
            try {
                // 追加到文件尾
                fw = new FileWriter(new File(profileDir + profileName), true);
                BufferedWriter w = new BufferedWriter(fw);
                w.write(line);
                w.close();
                fw.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @discribe 检查心跳信息
     * @return
     * @throws IOException
     */
    public void getActiveMMSByHB(ServerConf conf) throws IOException {
        List<String> ls = new ArrayList<>();
        Jedis jedis = null;
        try {
            jedis = StorePhoto.getRpL1(conf).getResource();
            Set<String> keys = jedis.keys("mm.hb.*");
            for (String hp : keys) {
                ls.add(hp.substring(6).split(":")[0] + ":" + conf.getHttpPort());
            }
            Map<Long, String> activeMap = new HashMap<>();
            for(String server : ls){
                for(Map.Entry<Long, String> entry : ServerConf.servers.entrySet()){
                    if(entry.getValue().equals(server)){
                        activeMap.put(entry.getKey(), server);
                    }
                }
            }
            synchronized (ServerConf.activeServers){
                ServerConf.activeServers.clear();
                ServerConf.activeServers.putAll(activeMap);
            }
        } catch (JedisException e) {
            logTool.error("Get mm.hb.* failed: " + e.getMessage());
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
    }
}
