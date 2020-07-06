package mammoth.server;

import mammoth.common.LogTool;
import mammoth.common.RedisPool;
import org.eclipse.jetty.server.Server;
import org.newsclub.net.unix.AFUNIXServerSocket;
import org.newsclub.net.unix.AFUNIXSocketAddress;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Tuple;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PhotoServer {
    private static LogTool logTool = new LogTool();
    public static long upts = System.currentTimeMillis();
    private ServerConf conf;
    private ServerSocket ss;
    private int serverport;
    private int period;
    private ExecutorService pool;
    // 集合跟到这个集合上的写操作队列的映射
    private ConcurrentHashMap<String, BlockingQueue<WriteTask>> sq =
            new ConcurrentHashMap<String, BlockingQueue<WriteTask>>();
    //	private LMDBInterface li = null;
//    private RocksDBInterface ri = null;

    public PhotoServer(ServerConf conf) throws Exception {
        this.conf = conf;
        serverport = conf.getServerPort();
        period = conf.getPeriod();
        if (!conf.isHTTPOnly()) {
            ss = new ServerSocket(serverport);
            pool = Executors.newCachedThreadPool();
        }
    }

    public void startUpHttp() throws Exception {
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();
        if (jedis != null) {
            Set<Tuple> active = jedis.zrangeWithScores("mm.active.http", 0, -1);
            if (active != null && active.size() > 0) {
                for (Tuple t : active) {
                    ServerConf.servers.put((long) t.getScore(), t.getElement());
                    logTool.info("Got HTTP Server " + (long) t.getScore() + " " +
                            t.getElement());
                }
            }
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }

        // 启动http服务
        Server server = new Server(conf.getHttpPort());
        server.setHandler(new HTTPHandler(conf));
        server.start();
    }

    public void startUp() throws Exception {
        // 服务端每隔一段时间进行一次读写速率统计,1秒后开始统计，每10秒输出一次平均信息
        Timer t = new Timer("ProfileTimer");
        t.schedule(new ProfileTimerTask(conf, period), 1 * 1000, period * 1000);

        // 修改了blk文件的命名策略，因此MMServer在启动后应当首先检查与自己相关的所有blk信息，
        // 确定是否需要对名字进行转换
        do {
            if (ServerConf.serverId != -1l) {
               // __upgrade_blk_info();
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
            }
        } while (true);

        // Server Health timer trigger every half interval seconds
        Timer t2 = new Timer("ServerHealth");
        ServerHealth sh = new ServerHealth(conf);
        t2.schedule(sh, 2 * 1000,
                Math.min(conf.getMemCheckInterval(),
                        conf.getSpaceOperationInterval()) / 2);
        // BUG-XXX: try to update space info ASAP, otherwise smart client
        // might not do obj writes.
        sh.gatherSpaceInfo(conf);

        // 启动http服务
        Server server = new Server(conf.getHttpPort());
        server.setHandler(new HTTPHandler(conf));
        server.start();
        // 计算图片hash值的线程
        //FeatureSearch im = new FeatureSearch(conf);
        //im.startWork(4);

        //TODO:统计功能使用了keys命令，慢查询会影响性能，关闭该功能。
        //入库统计http charts
        MMCountThread mmct = new MMCountThread(conf);
    	new Thread(mmct).start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logTool.info("Shutdown search server, release resources.");
//				Li.LMDBClose();
//                ri.rocksClose();
                StorePhoto.quit();
            }
        });

        // 启动监听写请求的服务,它使用junixsocket,所以需要用一个新的线程
      //  if (conf.isUse_junixsocket())
        //    new Thread(new WriteServer(), "WriteServerThread").start();

//        ri = new RocksDBInterface(conf);
        while (true) {
            try {
                Socket s = ss.accept();
                logTool.info("client connected -> " + s.getRemoteSocketAddress());
                DataInputStream dis = new DataInputStream(s.getInputStream());
                DataOutputStream dos = new DataOutputStream(s.getOutputStream());
                byte[] header = new byte[2];
                dis.readFully(header);
                int userNameLength = header[0];
                int passWordLength = header[1];
                String name = new String(readBytes(userNameLength, dis));
                String pwd = new String(readBytes(passWordLength, dis));
                if(!ServerConf.userName.equals(name) || !ServerConf.passWord.equals(pwd)){
                    dos.writeInt(-1);
                    s.close();
                    continue;
                }else{
                    dos.writeInt(1);
                }
                pool.execute(new Handler(conf, s, sq));
                        // 接收tcp请求,来自tcp的请求是读取请求或者写请求
            } catch (IOException e) {
                e.printStackTrace();
                // BUG-XXX: do not shutdown the pool on any IOException.
                //pool.shutdown();
            }
        }
    }
    public byte[] readBytes(int count, InputStream istream) throws IOException {
        byte[] buf = new byte[count];
        int n = 0;

        while (count > n) {
            n += istream.read(buf, n, count - n);
        }

        return buf;
    }

    /**
     * 将所有‘v1545267600.blk.SBZX-HXSJ-CL-0223./mmd1’格式的数据转成‘v1545267600.blk.2./mmd2’格式
     * @throws Exception
     */
    /*public void __upgrade_blk_info() throws Exception {
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();

        if (jedis == null) {
            throw new Exception("Could not get avaliable Jedis instance.");
        }
        try {
            String upgraded = jedis.hget("mm.s.upgrade", "blk." + ServerConf.serverId);

            for (Map.Entry<String, RedisPool> entry : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
                Jedis j = entry.getValue().getResource();
                if (j != null) {
                    try {
                        if (upgraded == null || !upgraded.equals("ok")) {
                            Set<String> keys = j.keys("*.blk.*");

                            if (keys != null && keys.size() > 0) {
                                for (String k : keys) {
                                    String[] ka = k.split("\\.");

                                    if (ka != null && ka.length == 4) {
                                        if (ka[2].equals(conf.getNodeName()) &&
                                                conf.getStoreArray().contains(ka[3])) {
                                            // update it to serverId
                                            String value = j.get(k);
                                            Pipeline p = j.pipelined();
                                            p.set(k.replace(conf.getNodeName(),
                                                    "" + ServerConf.serverId), value);
                                            p.del(k);
                                            p.sync();
                                        }
                                    }
                                }
                            }
                            jedis.hset("mm.s.upgrade", "blk." + ServerConf.serverId, "ok");
                        }
                    } finally {
                        entry.getValue().putInstance(j);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
    }*/

    public static String getDNSHtml(ServerConf conf) {
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();
        String r = "";

        if (jedis == null)
            return "#FAIL: Get default jedis instance failed.";

        try {
            // get dns info
            Map<String, String> dns = jedis.hgetAll("mm.dns");

            if (dns != null && dns.size() > 0) {
                for (Map.Entry<String, String> e : dns.entrySet()) {
                    r += e.getKey() + " -> " + e.getValue() + "<p/>";
                }
            } else {
                r += "Not available.<p>";
            }
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }
        return r;
    }

    public static String getServerInfoHtml(ServerConf conf) {
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();
        String r = "";

        if (jedis == null)
            return "#FAIL: Get default jedis instance failed.";

        try {
            // find all http servers
            int configNr = 0, activeNr = 0;
            Set<Tuple> cservers = jedis.zrangeWithScores("mm.active.http", 0, -1);//返回0到-1的所有值

            if (cservers != null) {
                r += "<H2> Total HTTP Servers: </H2><tt>";
                for (Tuple s : cservers) {
                    String[] url = s.getElement().split(":");
                    InetSocketAddress isa = new InetSocketAddress(url[0], 10000);
                    r += "<p> <a href=http://" + isa.getAddress().getHostAddress() +
                            ":" + url[1] + "/info><tt>[MMS" + (long) s.getScore() + "] " +
                            s.getElement() + "</tt></a>";
                }
                configNr = cservers.size();
            }

            // find all mms servers
            Set<Tuple> mservers = jedis.zrangeWithScores("mm.active", 0, -1);
            HashMap<String, Long> smap = new HashMap<String, Long>();
            Set<String> aservers = new HashSet<>();
            if (mservers != null) {
                for (Tuple s : mservers) {
                    smap.put(s.getElement(), (long) s.getScore());
                    if (jedis.exists("mm.hb." + s.getElement())){
                        aservers.add(s.getElement());
                    }
                }
            }

            // find heartbeated servers

            if (aservers != null) {
                r += "</tt><H2> Active MM Servers: </H2><tt>";
                for (String s : aservers) {
                    r += "<p>[MMS" + smap.get(s) + "] " + s;
                }
                r += "</tt>";
                activeNr = aservers.size();
            }
            if (activeNr < configNr) {
                r += "<H3><font color=\"red\">" + (configNr - activeNr) +
                        " MM Server(s) might Down!</font></H3>";
            }
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }

        return r;
    }

    public static String getSpaceInfoHtml(ServerConf conf) {
        String r = "";
        long free = 0;

        r += "<H2> Free Spaces (B): </H2><tt>";
        for (String dev : conf.getStoreArray()) {
            File f = new File(dev);
            r += "<p>" + dev + " -> " + "Total " + f.getTotalSpace() +
                    ", Free " + f.getUsableSpace();
            free += f.getUsableSpace();
        }
        r += "<p> [Total Free] " + free + " (B)</tt>";
        return r;
    }

    public static String getServerInfo(ServerConf conf) {
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();
        String r = "";

        if (jedis == null)
            return "#FAIL: Get default jedis instance failed.";

        try {
            // find all servers
            Set<String> servers = jedis.zrange("mm.active", 0, -1);
            Set<String> aServers = new HashSet<>();
            r += "\n Total  Servers:";
            for (String s : servers) {
                r += " " + s + ",";
                if (jedis.exists("mm.hb." + s)){
                    aServers.add(s);
                }
            }
            // find heartbeated servers
            r += "\n Active Servers:";
            for (String s : aServers) {
                r += " " + s + ",";
            }
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }

        return r;
    }
    
    public static String getHomeServerInfoHtml(ServerConf conf) {
        Jedis jedis = StorePhoto.getRpL1(conf).getResource();
        String r = "";

        if (jedis == null)
            return "#FAIL: Get default jedis instance failed.";

        try {
            int configNr = 0, activeNr = 0;
            Set<Tuple> cservers = jedis.zrangeWithScores("mm.active.http", 0, -1);//返回0到-1的所有值

            if (cservers != null) {
            	
                r += "<div class=\"col-sm-4 invoice-col\"><address><strong> Total HTTP Servers </strong><br>";
                for (Tuple s : cservers) {
                    String[] url = s.getElement().split(":");
                    InetSocketAddress isa = new InetSocketAddress(url[0], 10000);
                    r += "<a href=http://" + isa.getAddress().getHostAddress() +
                            ":" + url[1] + "/home>[MMS" + (long) s.getScore() + "] " +
                            s.getElement() + "</a><br>";
                }
                r += "</address></div>";
                configNr = cservers.size();
            }

            Set<Tuple> mservers = jedis.zrangeWithScores("mm.active", 0, -1);
            HashMap<String, Long> smap = new HashMap<String, Long>();
            Set<String> aservers = new HashSet<>();
            if (mservers != null) {
                for (Tuple s : mservers) {
                    smap.put(s.getElement(), (long) s.getScore());
                    if (jedis.exists("mm.hb." + s.getElement())){
                        aservers.add(s.getElement());
                    }
                }
            }
            if (aservers != null) {
                r += "<div class=\"col-sm-4 invoice-col\"><address><strong>Active MM Servers</strong><br>";
                for (String s : aservers) {
                    r += "[MMS" + smap.get(s) + "] " + s + "<br>";
                }
                r += "</address></div>";
                activeNr = aservers.size();
            }
            if (activeNr < configNr) {
                r += "<H3><font color=\"red\">" + (configNr - activeNr) +
                        " MM Server(s) might Down!</font></H3>";
            }
        } finally {
            StorePhoto.getRpL1(conf).putInstance(jedis);
        }

        return r;
    }

    public static String getHomeSpaceInfoHtml(ServerConf conf) {
        String r = "";

        r += "<div class=\"box-body\"><table class=\"table table-bordered\"><tbody>";
        r += "<tr><th>Path</th><th>Total(B)</th><th>Free(B)</th><th>Progress</th><th style=\"width: 40px\">Label</th></tr>";
        NumberFormat nt = NumberFormat.getPercentInstance();
        nt.setMinimumFractionDigits(0);
        for (String dev : conf.getStoreArray()) {
            File f = new File(dev);
            r += "<tr><td>" + dev + "</td><td>" + f.getTotalSpace() + "</td><td>" + f.getUsableSpace() + "</td>";
            String color = "<td><div class=\"progress progress-xs progress-striped active\">" +
                      "<div class=\"progress-bar progress-bar-success\" style=\"width: 0%\"></div>" +
                      "</div></td><td><span class=\"badge bg-green\">0%</span></td>";
    		float fs = 1F - (float)f.getUsableSpace() / f.getTotalSpace();   
			;
            if (fs <= 0.25) {
            	color = "<td><div class=\"progress progress-xs progress-striped active\">" +
                        "<div class=\"progress-bar progress-bar-success\" style=\"width: "+nt.format(fs)+"\"></div>" +
                        "</div></td><td><span class=\"badge bg-green\">"+nt.format(fs)+"</span></td>";
            } else if (fs <= 0.5) {
            	color = "<td><div class=\"progress progress-xs progress-striped active\">" +
            			"<div class=\"progress-bar progress-bar-primary\" style=\"width: "+nt.format(fs)+"\"></div>" +
            			"</div></td><td><span class=\"badge bg-blue\">"+nt.format(fs)+"</span></td>";
            } else if (fs <= 0.75) {
            	color = "<td><div class=\"progress progress-xs progress-striped active\">" +
            			"<div class=\"progress-bar progress-bar-yellow\" style=\"width: "+nt.format(fs)+"\"></div>" +
            			"</div></td><td><span class=\"badge bg-yellow\">"+nt.format(fs)+"</span></td>";
            } else if (fs <= 1) {
            	color = "<td><div class=\"progress progress-xs progress-striped active\">" +
                        "<div class=\"progress-bar progress-bar-danger\" style=\"width: "+nt.format(fs)+"\"></div>" +
                        "</div></td><td><span class=\"badge bg-red\">"+nt.format(fs)+"</span></td>";
			}
            r += color;
	}
            r += "</tbody></table></div>";
        return r;
    }


    /**
     * 专门用来接收写请求，使用junixsocket,应该可以实现并tcp更快的进程间通信
     *
     * @author zhaoyang
     */
    class WriteServer implements Runnable {
        @Override
        public void run() {
            //在本机部署多个服务端,要修改
            final File socketFile = new File(new File(System.getProperty("java.io.tmpdir")), "junixsocket-test.sock");
            ExecutorService pool = Executors.newCachedThreadPool();
            AFUNIXServerSocket server;

            try {
                server = AFUNIXServerSocket.newInstance();
                server.bind(new AFUNIXSocketAddress(socketFile));
                logTool.info("Start Unix Socket Server @ " + server.getInetAddress());
                while (true) {
                    Socket sock = server.accept();
                    pool.execute((new Handler(conf, sock, sq)));
                }
            } catch (Exception e) {
                e.printStackTrace();
                pool.shutdown();
            }
        }
    }

}

