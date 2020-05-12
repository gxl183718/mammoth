package mammoth.jclient;


import java.io.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


public class MMSClient {

    public static class Option {
        String flag, opt;

        public Option(String flag, String opt) {
            this.flag = flag;
            this.opt = opt;
        }
    }

    public static class LPutThread extends Thread {
        private ClientAPI ca;
        public long pnr = 0L;
        public long size = 0L;
        public String type = "";
        public String set = null;
        public long apnr = 0L;
        public long begin, end;

        public LPutThread(ClientAPI ca, String set, long pnr, long size, String type) {
            this.pnr = pnr;
            this.size = size;
            this.type = type;
            this.set = set;
            if (type.equalsIgnoreCase("pthca")) {
                try {
                    this.ca = new ClientAPI();
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("Fallback to use one ClientAPI");
                    this.ca = ca;
                    return;
                }
                String uri = "STL://";
                for (String s : ca.getPc().getConf().getSentinels())
                    uri = uri + s + ";";
                try {
                    this.ca.init(uri.substring(0, uri.length() - 1));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else
                this.ca = ca;
        }

        public void run() {
            begin = System.nanoTime();
            try {
                if (type.startsWith("mput")) {
                    MessageDigest md;
                    md = MessageDigest.getInstance("md5");

                    int pack = Integer.parseInt(type.split("_")[1]);
                    Random r = new Random();
                    for (int i = 0; i < pnr / pack; i++) {
                        byte[][] content = new byte[pack][(int) size];
                        String[] md5s = new String[pack];
                        for (int l = 0; l < pack; l++) {
                            r.nextBytes(content[l]);
                            md.update(content[l]);
                            byte[] mdbytes = md.digest();
                            StringBuffer sb = new StringBuffer();
                            for (int j = 0; j < mdbytes.length; j++) {
                                sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                            }
                            md5s[l] = sb.toString();
                        }
                        ca.mPut(set, md5s, content);
                    }
                } else {
                    for (int i = 0; i < pnr; i++) {
                        byte[] content = new byte[(int) size];
                        Random r = new Random();
                        r.nextBytes(content);
                        MessageDigest md;
                        md = MessageDigest.getInstance("md5");
                        md.update(content);
                        byte[] mdbytes = md.digest();

                        StringBuffer sb = new StringBuffer();
                        for (int j = 0; j < mdbytes.length; j++) {
                            sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                        }
                        try {
                            if (type.equalsIgnoreCase("sync")){
				long start = System.currentTimeMillis();
                                //System.out.println("KEY=" + set + "@" + sb.toString() + ", INFO=" + ca.put(set + "@" + sb.toString(), content));
                                ca.put(set + "@" + sb.toString(), content);
				System.out.println("put " + set + "@" + sb.toString() + " used " + (System.currentTimeMillis() - start ) + " ms");
                            }else if (type.equalsIgnoreCase("async"))
                                ca.put(set + "@" + sb.toString(), content);
                            else if (type.equalsIgnoreCase("pthca"))
                                ca.put(set + "@" + sb.toString(), content);
                            else {
                                if (type.equals(""))
                                    System.out.println("Please provide lpt_type");
                                else
                                    System.out.println("Wrong lpt_type, should be sync, async, pthca or mput_{pack}");
                                System.exit(0);
                            }
                            apnr++;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
                end = System.nanoTime();
                System.out.println(Thread.currentThread().getId() + " --> Put " + apnr + " objects in " +
                        ((end - begin) / 1000.0) + " us, PPS is " + (pnr * 1000000000.0) / (end - begin));
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class LGetThread extends Thread {
        private ClientAPI ca;
        public Set<String> gets;
        public long gnr = 0;
        public long size = 0;
        public boolean doCheck;

        public long begin, end;

        public LGetThread(ClientAPI ca, Set<String> gets, boolean doCheck) {
            this.ca = ca;
            this.gets = gets;
            this.doCheck = doCheck;
        }

        public void run() {
            begin = System.nanoTime();
            for (String key : gets) {
                byte[] rr = null;
                try {
                    rr = ca.get(key);
                    byte[] r = rr;
                    if (doCheck) {
                        MessageDigest md;
                        md = MessageDigest.getInstance("md5");
                        md.update(r);
                        byte[] mdbytes = md.digest();
                        StringBuffer sb = new StringBuffer();
                        for (int j = 0; j < mdbytes.length; j++) {
                            sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                        }
                        String[] x = key.split("@");
                        if (!sb.toString().equalsIgnoreCase(x[1])) {
                            System.out.println("Key " + key + " is corrupt.");
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (rr != null)
                    size += rr.length;
                gnr++;
            }
            end = System.nanoTime();
            System.out.println(Thread.currentThread().getId() + " --> Get " + gnr + " objects in " +
                    ((end - begin) / 1000.0) + " us, GPS is " + (gnr * 1000000000.0) / (end - begin) + ", checked=" + doCheck);
        }
    }

    public static class LMGetThread extends Thread {
        private ClientAPI ca;
        public List<String> keys;
        public long gnr = 0;
        public long size = 0;
        public boolean doCheck;

        public long begin, end;

        public LMGetThread(ClientAPI ca, List<String> keys, boolean doCheck) {
            this.ca = ca;
            this.keys = keys;
            this.doCheck = doCheck;
        }

        public void run() {
            begin = System.nanoTime();

            try {
                Map<String, String> cookies = new HashMap<String, String>();
                long begin, end;

                long ttime = 0;
                do {
                    begin = System.nanoTime();
                    List<byte[]> b = ca.mget(keys, cookies);
                    end = System.nanoTime();
                    ttime += (end - begin);
                    gnr += b.size();
                    long len = 0;
                    for (byte[] v : b) {
                        if (v != null)
                            len += v.length;
                    }
                    System.out.println(Thread.currentThread().getId() + " --> Got nr " + b.size() + " vals len " + len + "B in " + ((end - begin) / 1000.0) + " us, BW is " + (len / ((end - begin) / 1000000.0)) + " KB/s.");
                    System.out.flush();
                    size += len;

                    // do check now
                    int idx = Integer.parseInt(cookies.get("idx"));
                    if (doCheck) {
                        for (int i = 0; i < b.size(); i++) {
                            MessageDigest md;
                            md = MessageDigest.getInstance("md5");
                            md.update(b.get(i));
                            byte[] mdbytes = md.digest();

                            StringBuffer sb = new StringBuffer();
                            for (int j = 0; j < mdbytes.length; j++) {
                                sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                            }
                            if (!sb.toString().equalsIgnoreCase(keys.get(i + (idx - b.size())).split("@")[1])) {
                                System.out.println(Thread.currentThread().getId() + " -->IDX " + i + " : expect " + keys.get(i) + " got " + sb.toString());
                            }
                        }
                    }
                    //if (idx >= r.size() - 1)
                    if (b.size() == 0)
                        break;
                } while (true);

                System.out.println(Thread.currentThread().getId() + " --> cookies: " + cookies);
                System.out.println(Thread.currentThread().getId() + " --> MGET nr " + keys.size() + " size " + size + "B : BW " +
                        (size / (ttime / 1000000.0)) + " KB/s");

            } catch (IOException e) {
                e.printStackTrace();
            } catch (Exception e) {
                e.printStackTrace();
            }

            end = System.nanoTime();
            System.out.println(Thread.currentThread().getId() + " --> MGet " + gnr + " objects in " +
                    ((end - begin) / 1000.0) + " us, GPS is " + (gnr * 1000000000.0) / (end - begin) + ", checked=" + doCheck);
        }
    }

    /**
     * @param args
     */
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

        String set = "default";
        ClientConf.MODE mode = ClientConf.MODE.NODEDUP;
        long lpt_nr = 1, lpt_size = 1;
        int lgt_nr = -1, lgt_th = 1, lpt_th = 1, lmgt_th = 1;
        String lpt_type = "", lgt_type = "";
        int lmpt_nr = -1, lmpt_pack = -1, lmpt_size = -1;
        boolean lgt_docheck = false;
        int dupNum = 1;
        Set<String> sentinels = new HashSet<String>();
        String uri = null;
        String mget_type = "all";
        long mget_begin_time = 0;
        List<String> osServers = null;

        for (Option o : optsList) {
            if (o.flag.equals("-h")) {
                // print help message
                System.out.println("-h    : print this help.");
                System.out.println("-m    : client operation mode.");
                System.out.println("-dn   : duplication number.");

                System.out.println("-set  : specify set name.");
                System.out.println("-put  : put an object to server.");
                System.out.println("-get  : get an object from server by md5.");
                System.out.println("-getbi: get an object from server by INFO.");
                System.out.println("-del  : delete set from server.");

                System.out.println("-lpt  : large scacle put test.");
                System.out.println("-lgt  : large scacle get test.");
                System.out.println("-lmpt : large scale mput test");
                System.out.println("-getserverinfo  :  get info from all servers online");

                System.out.println("-stl  : sentinels <host:port;host:port>.");

                System.out.println("-uri  : unified uri for SENTINEL and STANDALONE.");

                System.exit(0);
            }
            if (o.flag.equals("-m")) {
                // set client mode
                if (o.opt == null) {
                    System.out.println("Please specify mode: [dedup, nodedup]");
                } else {
                    if (o.opt.equalsIgnoreCase("dedup")) {
                        mode = ClientConf.MODE.DEDUP;
                    } else if (o.opt.equalsIgnoreCase("nodedup")) {
                        mode = ClientConf.MODE.NODEDUP;
                    }
                }
            }
            if (o.flag.equals("-dn")) {
                //set duplication number
                if (o.opt == null) {
                    System.out.println("Please specify dn, or 1 is set by default");
                } else {
                    dupNum = Integer.parseInt(o.opt);
                    if (dupNum < 0) {
                        System.out.println("dn must be positive.");
                        System.exit(0);
                    }
                }
            }
            if (o.flag.equals("-set")) {
                // set the set name
                set = o.opt;
            }
            if (o.flag.equals("-lpt_nr")) {
                // set ltp nr
                lpt_nr = Long.parseLong(o.opt);
            }
            if (o.flag.equals("-lpt_size")) {
                // set lpt size
                lpt_size = Long.parseLong(o.opt);
            }
            if (o.flag.equals("-lpt_type")) {
                //sync or async
                lpt_type = o.opt;
            }
            if (o.flag.equals("-lgt_nr")) {
                lgt_nr = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-lgt_th")) {
                lgt_th = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-lmgt_th")) {
                lmgt_th = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-lpt_th")) {
                lpt_th = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-lgt_docheck")) {
                lgt_docheck = true;
            }
            if (o.flag.equals("-lgt_type")) {
                // get or search
                lgt_type = o.opt;
            }
            if (o.flag.equals("-lmpt_nr")) {
                lmpt_nr = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-lmpt_pack")) {
                lmpt_pack = Integer.parseInt(o.opt);
            }
            if (o.flag.equals("-lmpt_size")) {
                lmpt_size = Integer.parseInt(o.opt);
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
                }
            }
            if (o.flag.equals("-uri")) {
                // parse uri
                if (o.opt == null) {
                    System.out.println("-uri URI");
                    System.exit(0);
                }
                uri = o.opt;
            }
            if (o.flag.equals("-mget_type")) {
                if (o.opt == null) {
                    System.out.println("-mget_type TYPE");
                    System.exit(0);
                }
                mget_type = o.opt;
            }
            if (o.flag.equals("-mget_begin_time")) {
                if (o.opt == null) {
                    System.out.println("-mget_begin_time TIME");
                    System.exit(0);
                }
                mget_begin_time = Long.parseLong(o.opt);
            }
            if (o.flag.equals("-os_servers")) {
                if (o.opt == null) {
                    System.out.println("-os_servers SERVER");
                    System.exit(0);
                }
                String[] _t = o.opt.split(";");
                if (_t != null) {
                    for (String t : _t) {
                        if (t != null && t.contains(":")) {
                            if (osServers == null)
                                osServers = new ArrayList<String>();
                            osServers.add(t);
                        }
                    }
                }
            }
        }

		/*ClientConf conf = null;
        try {
			if (sentinels.size() > 0)
				conf = new ClientConf(sentinels, mode, dupNum);
			else
				conf = new ClientConf(serverName, serverPort, redisHost, redisPort, mode, dupNum);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			System.exit(0);
		}*/
        ClientAPI pcInfo = null;
        try {
            //pcInfo = new ClientAPI(conf);
            //pcInfo.init(redisHost + ":" + redisPort);
            pcInfo = new ClientAPI();
            pcInfo.init(uri);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }

        for (Option o : optsList) {
            if (o.flag.equals("-lput")) {
                // loop put test
                if (o.opt == null) {
                    System.out.println("Please provide the put target.");
                    System.exit(0);
                }
                pcInfo.getPc().getConf().setPrintServerRefresh(true);
                while (true) {
                    byte[] content = null;
                    File f = new File(o.opt);
                    if (f.exists()) {
                        try {
                            FileInputStream in = new FileInputStream(f);
                            content = new byte[(int) f.length()];
                            in.read(content);
                            in.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    try {
                        MessageDigest md;
                        md = MessageDigest.getInstance("md5");
                        md.update(content);
                        byte[] mdbytes = md.digest();

                        StringBuffer sb = new StringBuffer();
                        for (int j = 0; j < mdbytes.length; j++) {
                            sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                        }
                        System.out.println("MD5: " + sb.toString() + " -> INFO: " + pcInfo.put(set + "@" + sb.toString(), content));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (o.flag.equals("-getinfo")) {
                if (o.opt == null) {
                    System.out.println("Please provide the get md5.");
                    System.exit(0);
                }
                String md5 = o.opt;
                System.out.println("Provide the set and md5 about a photo.");
                System.out.println("get args: set " + set + ", md5 " + md5);

                try {
                    FileInfo fi = pcInfo.getFile(set + "@" + md5);
                    System.out.println(fi);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-put")) {
                if (o.opt == null) {
                    System.out.println("Please provide the put target.");
                    System.exit(0);
                }
                File f = new File(o.opt);
                try {
                    if (f.exists()) {
                        long start = System.currentTimeMillis();
                            System.out.println("KEY: " + pcInfo.uploadFile(f, set));
                        System.out.println("Total time :" + ((System.currentTimeMillis() - start) / 1000) + "s");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-lpta")) {
                // large scale put test with atomic interface
                System.out.println("Provide the number of iterations, and mm object size.");
                System.out.println("LPT args: nr " + lpt_nr + ", size " + lpt_size + ", type " + lpt_type);

                List<LPutThread> lputs = new ArrayList<LPutThread>();
                for (int i = 0; i < lpt_th; i++) {
                    lputs.add(new LPutThread(pcInfo, set, lpt_nr / lpt_th, lpt_size, lpt_type));
                }
                long begin = System.currentTimeMillis();
                for (LPutThread t : lputs) {
                    t.start();
                }
                lpt_nr = 0;
                for (LPutThread t : lputs) {
                    try {
                        t.join();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    lpt_nr += t.pnr;
                }
                long end = System.currentTimeMillis();
                System.out.println("LPTA nr " + lpt_nr + " size " + lpt_size +
                        ": BW " + lpt_size * lpt_nr * 1000.0 / 1024.0 / (end - begin) + " KBps," +
                        " LAT " + (end - begin) / (double) lpt_nr + " ms");
            }
            if (o.flag.equals("-lpt")) {
                // large scale put test
                System.out.println("Provide the number of iterations, and mm object size.");
                System.out.println("LPT args: nr " + lpt_nr + ", size " + lpt_size + ", type " + lpt_type);

                try {
                    long begin = System.currentTimeMillis();

                    for (int i = 0; i < lpt_nr; i++) {
                        byte[] content = new byte[(int) lpt_size];
                        Random r = new Random();
                        r.nextBytes(content);
                        MessageDigest md;
                        md = MessageDigest.getInstance("md5");
                        md.update(content);
                        byte[] mdbytes = md.digest();

                        StringBuffer sb = new StringBuffer();
                        for (int j = 0; j < mdbytes.length; j++) {
                            sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                        }
                        if (lpt_type.equalsIgnoreCase("sync"))
//                            System.out.println(pcInfo.put( content, ClientAPI.MMType.VIDEO));
                            System.out.println();
                        else if (lpt_type.equalsIgnoreCase("async"))
                            pcInfo.put(set + "@" + sb.toString(), content);
                        else {
                            if (lpt_type.equals(""))
                                System.out.println("Please provide lpt_type");
                            else
                                System.out.println("Wrong lpt_type, should be sync or async");
                            System.exit(0);
                        }

                        if (i % 100 == 0) {
                            long temp = System.currentTimeMillis();
                            System.out.println("[PUT] nr = " + i + " time : " + (temp - begin) + " ms");
                        }
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("LPT nr " + lpt_nr + " size " + lpt_size +
                            ": BW " + lpt_size * lpt_nr * 1000.0 / 1024.0 / (end - begin) + " KBps," +
                            " LAT " + (end - begin) / (double) lpt_nr + " ms");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (o.flag.equals("-lgta")) {
                // large scale get test with atomic interface
                System.out.println("Provide the number of iterations.");
                System.out.println("LGT args: nr " + lgt_nr);
                if (lgt_nr == -1) {
                    System.out.println("please provide number of iterations using -lgt_nr");
                    System.exit(0);
                }
                try {
                    Map<String, String> stored = pcInfo.getPc().getNrFromSet(set);
                    if (stored.size() < lgt_nr) {
                        lgt_nr = stored.size();
                    }
                    int i = 0;
                    long size = 0;
                    ArrayList<Set<String>> gets = new ArrayList<Set<String>>();
                    List<LGetThread> lgets = new ArrayList<LGetThread>();
                    for (i = 0; i < lgt_th; i++) {
                        gets.add(new HashSet<String>());
                        lgets.add(new LGetThread(pcInfo, gets.get(i), lgt_docheck));
                    }
                    i = 0;
                    if (lgt_type.equalsIgnoreCase("get")) {
                        for (String key : stored.keySet()) {
                            gets.get(i % lgt_th).add(set + "@" + key);
                            i++;
                            if (i >= lgt_nr)
                                break;
                        }
                    } else if (lgt_type.equalsIgnoreCase("search")) {
                        for (String val : stored.values()) {
                            gets.get(i % lgt_th).add(val);
                            i++;
                            if (i >= lgt_nr)
                                break;
                        }
                    } else {
                        if (lgt_type.equals(""))
                            System.out.println("Please provide lgt_type.");
                        else
                            System.out.println("Wrong lgt_type, expect 'get' or 'search'");
                        System.exit(0);
                    }
                    long begin = System.currentTimeMillis();
                    for (LGetThread t : lgets) {
                        t.start();
                    }
                    lgt_nr = 0;
                    size = 0;
                    for (LGetThread t : lgets) {
                        try {
                            t.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        lgt_nr += t.gnr;
                        size += t.size;
                    }
                    long dur = System.currentTimeMillis() - begin;
                    System.out.println("LGTA nr " + lgt_nr + " size " + size + "B " +
                            ": BW " + (size * 1000 / 1024.0 / (dur)) + " KBps," +
                            " AVG LAT " + ((double) dur / lgt_nr) + " ms");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (o.flag.equals("-lgt")) {
                // large scale get test
                System.out.println("Provide the number of iterations.");
                System.out.println("LGT args: nr " + lgt_nr);
                if (lgt_nr == -1) {
                    System.out.println("please provide number of iterations using -lgt_nr");
                    System.exit(0);
                }
                try {
                    Map<String, String> stored = pcInfo.getPc().getNrFromSet(set);
                    if (stored.size() < lgt_nr) {
                        lgt_nr = stored.size();
                    }
                    int i = 0;
                    long size = 0;
                    long begin = System.currentTimeMillis();

                    if (lgt_type.equalsIgnoreCase("get")) {
                        for (String key : stored.keySet()) {
                            byte[] r = pcInfo.get(set + "@" + key);
                            if (r != null)
                                size += r.length;
                            i++;
                            if (i >= lgt_nr)
                                break;
                        }
                    } else if (lgt_type.equalsIgnoreCase("search")) {
                        for (String val : stored.values()) {
                            byte[] r = pcInfo.get(val);
                            if (r != null)
                                size += r.length;
                            i++;
                            if (i >= lgt_nr)
                                break;
                        }
                    } else {
                        if (lgt_type.equals(""))
                            System.out.println("Please provide lgt_type.");
                        else
                            System.out.println("Wrong lgt_type, expect 'get' or 'search'");
                        System.exit(0);
                    }
                    long dur = System.currentTimeMillis() - begin;
                    System.out.println("LGT nr " + lgt_nr + " size " + size + "B " +
                            ": BW " + (size * 1000 / 1024.0 / (dur)) + " KBps," +
                            " LAT " + ((double) dur / lgt_nr) + " ms");
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-lmgt")) {
                System.out.println("Provide the set name, type(text,image,audio,video,application,thumbnail,other) and begin_time.");
                System.out.println("get args: type " + mget_type + ", begin_time " + mget_begin_time + ", docheck=" + lgt_docheck + ", lmgt_th=" + lmgt_th);

                List<String> r;
                try {
                    String rset = null;
                    long begin, end;

                    begin = System.currentTimeMillis();
                    r = pcInfo.getkeys(mget_type, mget_begin_time);
                    end = System.currentTimeMillis();
                    if (r.size() > 0)
                        rset = r.get(0).split("@")[0];
                    System.out.println("Got nr " + r.size() + " keys from Set " + rset + " in " + ((end - begin)) + " ms.");

                    List<LMGetThread> lgets = new ArrayList<LMGetThread>();
                    int n = r.size() / lmgt_th;
                    for (int i = 0; i < lmgt_th; i++) {
                        lgets.add(new LMGetThread(pcInfo, r.subList(i * n, (i + 1) * n), lgt_docheck));
                    }

                    for (LMGetThread t : lgets) {
                        t.start();
                    }
                    long nr = 0, size = 0;
                    for (LMGetThread t : lgets) {
                        try {
                            t.join();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        nr += t.gnr;
                        size += t.size;
                    }
                    long dur = System.currentTimeMillis() - begin;
                    System.out.println("LGTA nr " + nr + " size " + size + "B " +
                            ": BW " + (size * 1000 / 1024.0 / (dur)) + " KBps," +
                            " AVG LAT " + ((double) dur / nr) + " ms");

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (o.flag.equals("-lmpt")) {
                if (lmpt_nr < 0 || lmpt_pack < 0 || lmpt_size < 0) {
                    System.out.println("please provide lmpt_nr,lmpt_pack,lmpt_size");
                    System.exit(0);
                }
                System.out.println("LMPT args: nr " + lmpt_nr + ", size " + lmpt_size + ", package number " + lmpt_pack);

                try {
                    long begin = System.currentTimeMillis();
                    Random r = new Random();
                    MessageDigest md;
                    md = MessageDigest.getInstance("md5");

                    for (int i = 0; i < lmpt_nr / lmpt_pack; i++) {
                        byte[][] content = new byte[lmpt_pack][lmpt_size];
                        String[] md5s = new String[lmpt_pack];
                        for (int l = 0; l < lmpt_pack; l++) {
                            r.nextBytes(content[l]);
                            md.update(content[l]);
                            byte[] mdbytes = md.digest();
                            StringBuffer sb = new StringBuffer();
                            for (int j = 0; j < mdbytes.length; j++) {
                                sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                            }
                            md5s[l] = sb.toString();
                        }
                        pcInfo.mPut(set, md5s, content);
                    }
                    long end = System.currentTimeMillis();
                    System.out.println("LPT nr " + lmpt_nr + " size " + lmpt_size +
                            ": BW " + lmpt_size * lmpt_nr * 1000.0 / 1024.0 / (end - begin) + " KBps," +
                            " LAT " + (end - begin) / (double) lmpt_nr + " ms");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-get")) {
                if (o.opt == null) {
                    System.out.println("Please provide the get md5.");
                    System.exit(0);
                }
                String md5 = o.opt;
                System.out.println("Provide the set and md5 about a photo.");
                System.out.println("get args: set " + set + ", md5 " + md5);

                try {
                    byte[] content = pcInfo.get(set + "@" + md5);
                    FileOutputStream fos = new FileOutputStream(md5);
                    fos.write(content);
                    fos.close();
                    System.out.println("Get content length: " + content.length);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (o.flag.equals("-getbi")) {
                String info = o.opt;
                System.out.println("Provide the INFO about a photo.");
                System.out.println("get args:  " + info);
                try {
                    byte[] content = pcInfo.get(info);
                    FileOutputStream fos = new FileOutputStream("getbi");
                    fos.write(content);
                    fos.close();
                    System.out.println("get content length:" + content.length);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-mget")) {
                System.out.println("Provide the set name, type(text,image,audio,video,application,thumbnail,other) and begin_time.");
                System.out.println("get args: " + set + ", type " + mget_type + ", begin_time " + mget_begin_time + ", docheck=" + lgt_docheck);

                List<String> r;
                try {
                    String rset = null;
                    Map<String, String> cookies = new HashMap<String, String>();
                    long begin, end;

                    begin = System.nanoTime();
                    r = pcInfo.getkeys(mget_type, mget_begin_time);
                    end = System.nanoTime();
                    if (r.size() > 0)
                        rset = r.get(0).split("@")[0];
                    System.out.println("Got nr " + r.size() + " keys from Set " + rset + " in " + ((end - begin) / 1000.0) + " us.");

                    long tlen = 0;
                    long ttime = 0;
                    do {
                        begin = System.nanoTime();
                        List<byte[]> b = pcInfo.mget(r, cookies);
                        end = System.nanoTime();
                        ttime += (end - begin);

                        long len = 0;
                        for (byte[] v : b) {
                            if (v != null)
                                len += v.length;
                        }
                        System.out.println("Got nr " + b.size() + " vals len " + len + "B in " + ((end - begin) / 1000.0) + " us, BW is " + (len / ((end - begin) / 1000000.0)) + " KB/s.");
                        System.out.flush();
                        tlen += len;

                        // do check now
                        int idx = Integer.parseInt(cookies.get("idx"));
                        if (lgt_docheck) {
                            for (int i = 0; i < b.size(); i++) {
                                MessageDigest md;
                                md = MessageDigest.getInstance("md5");
                                md.update(b.get(i));
                                byte[] mdbytes = md.digest();

                                StringBuffer sb = new StringBuffer();
                                for (int j = 0; j < mdbytes.length; j++) {
                                    sb.append(Integer.toString((mdbytes[j] & 0xff) + 0x100, 16).substring(1));
                                }
                                if (!sb.toString().equalsIgnoreCase(r.get(i + (idx - b.size())).split("@")[1])) {
                                    System.out.println("IDX " + i + " : expect " + r.get(i) + " got " + sb.toString());
                                }
                            }
                        }
                        //if (idx >= r.size() - 1)
                        if (b.size() == 0)
                            break;
                    } while (true);

                    System.out.println(" -> cookies: " + cookies);
                    System.out.println("MGET nr " + r.size() + " size " + tlen + "B : BW " +
                            (tlen / (ttime / 1000000.0)) + " KB/s");

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-scrub")) {
                if (o.opt == null) {
                    System.out.println("Provide server:port to scrub.");
                    break;
                }
                System.out.println("get args: " + o.opt);
                String[] s = o.opt.split(":");
                if (s.length == 2) {
                    try {
                        pcInfo.getPc().scrubMetadata(s[0], Integer.parseInt(s[1]));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (o.flag.equals("-deleteFile")) {
                String md5 = o.opt;
                System.out.println("get args: set name  =" + set + "  md5 =" + md5);
                try {
                    pcInfo.deleteFile(set + "@" + md5);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-del")) {
                String sname = o.opt;
                System.out.println("Provide the set name to be deleted.");
                System.out.println("get args: set name  " + sname);
                DeleteSet ds = new DeleteSet(pcInfo);
                try {
                    ds.delSet(sname);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-recycle")) {
                String daystr = o.opt;
                System.out.println("Provide the last day to be deleted.");
                System.out.println("get args: day str= " + daystr);
                DeleteSet ds = new DeleteSet(pcInfo);
                try {
                    ds.recycleSet(daystr);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            if (o.flag.equals("-getserverinfo")) {
                System.out.println("get server info.");
                DeleteSet ds = new DeleteSet(pcInfo);
                List<String> ls = null;
                try {
                    ls = ds.getAllServerInfo();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                if (ls == null) {
                    System.out.println("出现错误");
                    return;
                }
                for (String s : ls) {
                    System.out.println(s);
                }
            }
        }
//		if (pcInfo.getPc().getConf().getRedisMode() == ClientConf.RedisMode.SENTINEL)
//			pcInfo.getPc().getRf().quit();
        pcInfo.quit();
    }

}
