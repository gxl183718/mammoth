package mammoth.server;

import mammoth.jclient.Feature.FeatureTypeString;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.*;

public class MMServer {
    public static ServerConf conf;

    public static class Option {
        String flag, opt;

        public Option(String flag, String opt) {
            this.flag = flag;
            this.opt = opt;
        }
    }

     public static String getHostIPByHint(String hint) throws SocketException {
        String node = null;

        Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            NetworkInterface n = (NetworkInterface) e.nextElement();
            Enumeration<InetAddress> ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                InetAddress i = (InetAddress) ee.nextElement();
                if (i.getHostAddress().contains(hint)) {
                    node = i.getHostAddress();
                    break;
                } 
            }
            if (node != null)
                break;
        }

        return node;
    }

    public static void main(String[] args) {

        List<String> argsList = new ArrayList<String>();
        List<Option> optsList = new ArrayList<Option>();
        List<String> doubleOptsList = new ArrayList<String>();

        // parse the args
        for (int i = 0; i < args.length; i++) {
            System.out.println("Args " + i + ", " + args[i]);
            switch (args[i].charAt(0)) {
                case '-':
                    if (args[i].length() < 2)
                        throw new IllegalArgumentException("Not a valid argument: " + args[i]);
                    if (args[i].charAt(1) == '-') {
                        if (args[i].length() < 3)
                            throw new IllegalArgumentException("Not a valid argument: " + args[i]);
                        doubleOptsList.add(args[i].substring(2, args[i].length()));
                    } else {
                        if (args.length - 1 > i)
                            if (args[i + 1].charAt(0) == '-') {
                                optsList.add(new Option(args[i], null));
                            } else {
                                optsList.add(new Option(args[i], args[i + 1]));
                                i++;
                            }
                        else {
                            optsList.add(new Option(args[i], null));
                        }
                    }
                    break;
                default:
                    // arg
                    argsList.add(args[i]);
                    break;
            }
        }

        String serverName = null, redisServer = null;
        String SysInfoStatServerName = null;
        int serverPort = ServerConf.DEFAULT_SERVER_PORT,
                blockSize = ServerConf.DEFAULT_BLOCK_SIZE,
                redisPort = ServerConf.DEFAULT_REDIS_PORT,
                period = ServerConf.DEFAULT_PERIOD,
                httpPort = ServerConf.DEFAULT_HTTP_PORT,
                SysInfoStatServerPort = ServerConf.DEFAULT_SYSINFOSTAT_PORT;
        Set<String> sa = new HashSet<>();
        Set<String> sentinels = new HashSet<>();
        String outsideIP = null;
        int delPercentage = 5;
        String faceDetectorXML = null;
        boolean isSetOutsideIP = false, indexFeatures = false;
        int wto = -1, rto = -1;
        boolean isSSMaster = false;
        boolean isLeSlave = false;
        boolean isVname = false;
        String lmdb_prefix = ".";
        ArrayList<String> rocks_prefix = new ArrayList<String>();
        long memorySize = 32 * 1024; //set memory size
        boolean enableMig = false;
        boolean rocks_swap = false;
        int verbose_delta = 0;
        int infolen = 9;

        for (Option o : optsList) {
            if (o.flag.equals("-h")) {
                // print help message
                System.out.println("-h     : print this help.");
                System.out.println("-r     : local server name.");
                System.out.println("-p     : local server listen port.");
                System.out.println("-rr    : redis server name.");
                System.out.println("-rp    : redis server port.");
                System.out.println("-sr    : SysInfoStat server name.");
                System.out.println("-sp    : SysInfoStat server port.");
                System.out.println("-hp    : http server port.");
                System.out.println("-blk   : block size.");
                System.out.println("-prd   : logging period.");
                System.out.println("-sa    : storage array.");
                System.out.println("-stl   : sentinels <host:port;host:port>.");
                System.out.println("-ip    : IP hint exported to outside service.");
                System.out.println("-http  : http mode only.");
                System.out.println("-wto   : write fd time out seconds.");
                System.out.println("-rto   : read  fd time out seconds.");
                System.out.println("-idx   : index image features.");
                System.out.println("-fXML  : face detector XML config file path.");
                System.out.println("-ssm   : enable secondary server master.");
                System.out.println("-lmdb  : set lmdb prefix path.");
                System.out.println("-rocks : set rocksdb prefix path.");
                System.out.println("-uip   : use IP as hostname.");
                System.out.println("-msize : set max memory size(in MB).");
                System.out.println("-mig   : enable migrations.");
                System.out.println("-dsp   : delect set Percentage.");
                System.out.println("-swap  : enable swapout.");
                System.out.println("-les   : enable levelDB slave.");
                System.out.println("-v     : set to verbose.");
                System.out.println("-vv    : set to more verbose.");
                System.out.println("-vvv   : set to most verbose.");

                System.exit(0);
            }
            if (o.flag.equals("-v")) {
                verbose_delta = 1;
            }
            if (o.flag.equals("-vv")) {
                verbose_delta = 2;
            }
            if (o.flag.equals("-vvv")) {
                verbose_delta = 3;
            }
            if (o.flag.equals("-ip")) {
                // set outside accessible IP address hint
                if (o.opt == null) {
                    System.out.println("-ip IPAddressHint");
                    System.exit(0);
                }
                outsideIP = o.opt;
            }
            if (o.flag.equals("-dsp")) {
                delPercentage = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-swap")) {
                rocks_swap = true;
            }
            if (o.flag.equals("-r")) {
                // set serverName
                if (o.opt == null) {
                    System.out.println("-r serverName");
                    System.exit(0);
                }
                serverName = o.opt;
            }
            if (o.flag.equals("-p")) {
                // set serverPort
                if (o.opt == null) {
                    System.out.println("-p serverPort");
                    System.exit(0);
                }
                serverPort = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-rr")) {
                // set redis server name
                if (o.opt == null) {
                    System.out.println("-rr redisServerName");
                    System.exit(0);
                }
                redisServer = o.opt;
            }
            if (o.flag.equals("-rp")) {
                // set redis server port
                if (o.opt == null) {
                    System.out.println("-rp redisServerPort");
                    System.exit(0);
                }
                redisPort = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-sr")) {
                // set SysInfoStat server name
                if (o.opt == null) {
                    System.out.println("-sr SysInfoStat_server_name");
                    System.exit(0);
                }
                SysInfoStatServerName = o.opt;
            }
            if (o.flag.equals("-sp")) {
                // set SysInfoStat server port
                if (o.opt == null) {
                    System.out.println("-sp SysInfoStat_server_port");
                    System.exit(0);
                }
                SysInfoStatServerPort = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-hp")) {
                // set http server port
                httpPort = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-blk")) {
                // set block size
                if (o.opt == null) {
                    System.out.println("-blk blockSize");
                    System.exit(0);
                }
                blockSize = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-prd")) {
                // set logging period
                if (o.opt == null) {
                    System.out.println("-prd period");
                    System.exit(0);
                }
                period = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-sa")) {
                // parse storage array by ';'
                if (o.opt == null) {
                    System.out.println("-sa path;path;path");
                    System.exit(0);
                }
                System.out.println("Got storage array: " + o.opt);
                String[] paths = o.opt.split(";");
                for (int i = 0; i < paths.length; i++) {
                    sa.add(paths[i]);
                }
            }
            if (o.flag.equals("-stl")) {
                // parse sentinels
                if (o.opt == null) {
                    System.out.println("-stl host:port;host:port");
                    System.exit(0);
                }
                String[] stls = o.opt.split(";");
                for (int i = 0; i < stls.length; i++) {
                    sentinels.add(stls[i]);
		    System.out.println("sentinel node : " + stls[i]);
                }
            }
            if (o.flag.equals("-wto")) {
                if (o.opt == null) {
                    System.out.println("-wto write_time_out_seconds");
                    System.exit(0);
                }
                wto = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-rto")) {
                if (o.opt == null) {
                    System.out.println("-rto read_time_out_seconds");
                    System.exit(0);
                }
                rto = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-idx")) {
                indexFeatures = true;
            }
            if (o.flag.equals("-fXML")) {
                if (o.opt == null) {
                    System.out.println("-fXML XML_FILE");
                    System.exit(0);
                }
                faceDetectorXML = o.opt;
            }
            if (o.flag.equals("-ssm")) {
            isSSMaster = true;
            }
            if (o.flag.equals("-les")) {
                isLeSlave = true;
            }
            if (o.flag.equals("-isvn")) {
                isVname = true;
                if (o.opt == null) {
                    System.out.println("info length : 9");
                } else {
                    infolen = Integer.parseInt(o.opt);
                    System.out.println("info length : " + infolen);
                }
            }
            if (o.flag.equals("-lmdb")) {
                if (o.opt == null) {
                    System.out.println("-lmdb LMDB_prefix_path");
                    System.exit(0);
                }
                lmdb_prefix = o.opt;
            }
            if (o.flag.equals("-rocks")) {
                if (o.opt == null) {
                    System.out.println("-rocksdb RocksDB_prefix_path");
                    System.exit(0);
                }
                String[] paths = o.opt.split(";");
                for (int i = 0; i < paths.length; i++) {
                    rocks_prefix.add(paths[i]);
                }
            }
            if (o.flag.equals("-msize")) {
                if (o.opt == null) {
                    System.out.println("-msize MEMORY_SIZE(MB)");
                    System.exit(0);
                }
                memorySize = Long.parseLong(o.opt);
            }
            if (o.flag.equals("-mig")) {
                enableMig = true;
            }
        }

        for (Option o : optsList) {
            if (o.flag.equals("-http")) {
                try {
                    conf = new ServerConf(httpPort);
                    conf.setHTTPOnly(true);
                    conf.setSentinels(sentinels);
                    PhotoServer ps = new PhotoServer(conf);
                    ps.startUpHttp();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.exit(0);
                }
                return;
            }
            if (o.flag.equals("-uip")) {
                try {
                    serverName = MMServer.getHostIPByHint(outsideIP);
                } catch (SocketException e) {
                    e.printStackTrace();
                }
            }
        }

        if (outsideIP == null) {
            System.out.println("In current version you HAVE TO set outside IP address (-ip IPADDR_HINT)(e.g. -ip .69.).");
            System.exit(0);
        } else {
//            try {
//                InetAddress[] a = InetAddress.getAllByName(InetAddress.getLocalHost().getHostName());
//                for (InetAddress ia : a) {
//                    if (ia.getHostAddress().contains(outsideIP)) {
//                        System.out.println("[1] Got host IP " + ia.getHostAddress() + " by hint " + outsideIP);
//                        outsideIP = ia.getHostAddress();
//                        isSetOutsideIP = true;
//                    }
//                }
//            } catch (UnknownHostException e) {
//                e.printStackTrace();
//                System.exit(0);
//            }
            String ip = System.getenv("MY_NODE_NAME");
            if (ip.contains(outsideIP)) {
                System.out.println("[1] Got host IP " + ip + " by hint POD ENV MY_NODE_NAME");
                outsideIP = ip;
                isSetOutsideIP = true;
            }
            if (!isSetOutsideIP) {
                System.out.println("not ip set");
            }
        }

        // set the serverConf
        try {
            if (sentinels.size() > 0)
                conf = new ServerConf(serverName, serverPort, sentinels, blockSize, period, httpPort);
            else
                conf = new ServerConf(serverName, serverPort, redisServer, redisPort, blockSize, period, httpPort);
            conf.setStoreArray(sa);
            conf.addToFeatures(FeatureTypeString.IMAGE_FACES);
            conf.addToFeatures(FeatureTypeString.IMAGE_LIRE);
            conf.setIndexFeatures(indexFeatures);
            conf.setFaceDetectorXML(faceDetectorXML);
            conf.setSSMaster(isSSMaster);
            conf.setLeSlave(isLeSlave);
            conf.setIsvname(isVname);
            if (isVname) {
                conf.setVlen(infolen);
            }
            conf.setLmdb_prefix(lmdb_prefix);
            if (conf.isSSMaster() || conf.isLeSlave())
	    conf.setRocks_prefix(rocks_prefix);
            conf.setMemorySize(memorySize);
            conf.setEnableSSMig(enableMig);
            conf.addVerbose(verbose_delta);
            conf.setDelPercentage(delPercentage);
            conf.setSwap(rocks_swap);
            System.out.println((isSSMaster ? "Enable" : "Disable") + " Secondary Server on current MMServer.");
            if (conf.getStoreArray().size() > 0) {
                conf.setFeatureIndexPath(conf.getStoreArray().toArray(new String[0])[0]);
            } else
                conf.setFeatureIndexPath(".");
            if (wto > 0)
                conf.setWrite_fd_recycle_to(wto * 1000);
            if (rto > 0)
                conf.setRead_fd_recycle_to(rto * 1000);
            if (isSetOutsideIP)
                conf.setOutsideIP(outsideIP);
            if (SysInfoStatServerName != null) {
                conf.setSysInfoServerName(SysInfoStatServerName);
                conf.setSysInfoServerPort(SysInfoStatServerPort);
                System.out.println("Enable SysInfoStat mode: Server " + SysInfoStatServerName + ":" + SysInfoStatServerPort);
            } else {
                System.out.println("Disable SysInfoStat mode");
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
        PhotoServer ps = null;
        try {
            ps = new PhotoServer(conf);
            ps.startUp();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

}
