package mammoth.server;

import mammoth.jclient.Feature;
import mammoth.jclient.ResultSet;
import mammoth.server.StorePhoto.ObjectContent;
import mammoth.server.StorePhoto.RedirectException;
import org.hyperic.sigar.SigarException;
import redis.clients.jedis.exceptions.JedisException;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.concurrent.atomic.AtomicInteger;

public class Handler implements Runnable {
    private ServerConf conf;
    private static ConcurrentHashMap<String, BlockingQueue<WriteTask>> sq;

    public static ConcurrentHashMap<String, BlockingQueue<WriteTask>> getWriteSq() {
        return sq;
    }

    private Socket s;

    private StorePhoto sp;

    private DataInputStream dis;
    private DataOutputStream dos; // 向客户端的输出流

    private static final ThreadLocal<byte[]> threadLocalRecvBuffer = new ThreadLocal<byte[]>() {
        @Override
        protected synchronized byte[] initialValue() {
            return new byte[ServerConf.getRecv_buffer_size()];
        }
    };

    private static final ThreadLocal<byte[]> threadLocalSendBuffer = new ThreadLocal<byte[]>() {
        @Override
        protected synchronized byte[] initialValue() {
            return new byte[ServerConf.getSend_buffer_size()];
        }
    };

    public Handler(ServerConf conf, Socket s,
                   ConcurrentHashMap<String, BlockingQueue<WriteTask>> sq)
            throws Exception {
        this.conf = conf;
        this.s = s;
        s.setTcpNoDelay(true);
        this.sq = sq;
        dis = new DataInputStream(this.s.getInputStream());
        dos = new DataOutputStream(this.s.getOutputStream());
        // BufferedInputStream(this.s.getInputStream()));
        // BufferedOutputStream(this.s.getOutputStream()));
        sp = new StorePhoto(conf);
    }

    private static AtomicInteger i = new AtomicInteger();

    @Override
    public void run() {
        Thread.currentThread().setName("HandlerThread-" + i.getAndIncrement());
        try {
            while (true) {
                byte[] header = new byte[4];
                dis.readFully(header);
                switch (header[0]) {
                    case ActionType.SYNCSTORE: {
                        int setlen = header[1];
                        int md5len = header[2];
                        int clen = dis.readInt();
			            int fnLen = dis.readInt();
                        // 一次把所有的都读出来,减少读取次数
			            byte[] bytes = readBytes(fnLen + setlen + md5len + clen, dis);
                        String fn = null;
                        if (fnLen > 0) {
                            fn = new String(bytes, 0, fnLen);
                        }
			String set = new String(bytes, fnLen, setlen);
                        String md5 = new String(bytes, fnLen + setlen, md5len);
                        String result = null;
                        try {
                            result = sp.storePhoto(set, md5, bytes, clen, fnLen + setlen + md5len, fn);
                        } catch (JedisException e) {
                            result = "#FAIL:" + e.getMessage();
                        }
                        if (result == null) {
                            dos.writeInt(-1);
                        } else {
                            dos.writeInt(result.getBytes().length);
                            dos.write(result.getBytes());
                        }
                        break;
                    }
                    //异步写
                    case ActionType.ASYNCSTORE: {
                        System.out.println();
                        int setlen = header[1];
                        int md5len = header[2];
                        int contentlen = dis.readInt();

                        // 一次把所有的都读出来,减少读取次数
                        byte[] setmd5content = readBytes(setlen + md5len + contentlen, dis);
                        String set = new String(setmd5content, 0, setlen);
                        String md5 = new String(setmd5content, setlen, md5len);

                        byte[] contents = readBytes(dis.readInt(), dis);

                        WriteTask t = new WriteTask(set, md5, setmd5content, setlen
                                + md5len, contentlen);
			            t.setFname("X");
                        BlockingQueue<WriteTask> bq = sq.get(set);

                        if (bq != null) {
                            // 存在这个键,表明该写线程已经存在,直接把任务加到任务队列里即可
                            bq.add(t);
                        } else {
                            // 如果不存在这个键,则需要新开启一个写线程
                            BlockingQueue<WriteTask> tasks = new LinkedBlockingQueue<>();
                            tasks.add(t);
                            sq.put(set, tasks);
                            WriteThread wt = new WriteThread(conf, set, sq);
                            new Thread(wt).start();
                        }
                        break;
                    }
                    //批量同步写
                    case ActionType.MPUT: {
                        int setlen = header[1];
                        int n = dis.readInt();
                        String set = new String(readBytesN(setlen, dis));
                        String[] md5s = new String[n];
                        int[] conlen = new int[n];
                        byte[][] content = new byte[n][];
                        for (int i = 0; i < n; i++) {
                            md5s[i] = new String(readBytesN(dis.readInt(), dis));
                        }
                        for (int i = 0; i < n; i++)
                            conlen[i] = dis.readInt();
                        for (int i = 0; i < n; i++) {
                            content[i] = readBytesN(conlen[i], dis);
                        }

                        String[] r = sp.mstorePhoto(set, md5s, content);
                        if (r == null)
                            dos.writeInt(-1);
                        else {
                            for (int i = 0; i < n; i++) {
                                dos.writeInt(r[i].getBytes().length);
                                dos.write(r[i].getBytes());
                            }
                        }
                        break;
                    }

                    case ActionType.SEARCH: {
                        // 这样能把byte当成无符号的用，拼接的元信息长度最大可以255
                        int infolen = header[1] & 0xff;
                        if (infolen > 0) {
                            String infos = new String(readBytes(infolen, dis), 0,
                                    infolen);
                            ObjectContent oc = null;
                            try {
                                oc = sp.searchPhoto(null, infos.split("@"),
                                        threadLocalSendBuffer.get());
                            } catch (RedirectException e) {
                            }
                            // FIXME: ??zhaoyang 有可能刚刚写进redis的时候，还无法马上读出来,
                            // 这时候会无法找到图片,返回null
                            if (oc != null && oc.content != null) {
                                dos.writeInt(oc.length);
                                dos.write(oc.content, 0, oc.length);
                            } else {
                                dos.writeInt(-1);
                            }
                        } else {
                            dos.writeInt(-1);
                        }
                        break;
                    }
                    case ActionType.IGET: {
                        int infolen = header[1] & 0xff;
                        int gid = dis.readInt();
                        int seqno = dis.readInt();
                        if (infolen > 0) {
                            String info = new String(readBytes(infolen, dis), 0,
                                    infolen);
                            ObjectContent oc = null;
                            try {
                                oc = sp.searchPhoto("", info.split("@"),
                                        threadLocalSendBuffer.get());
                            } catch (RedirectException e) {
                            }
                            if (oc != null && oc.content != null) {
                                dos.writeInt(gid);
                                dos.writeInt(seqno);
                                dos.writeInt(oc.length);
                                dos.write(oc.content, 0, oc.length);
                            } else {
                                dos.writeInt(-1);
                            }
                        } else {
                            dos.writeInt(-1);
                        }
                        break;
                    }
                    case ActionType.DELSET: {
                        String set = new String(readBytes(header[1], dis), 0,
                                header[1]);
                        System.out.println(s.getRemoteSocketAddress() + " do action 'DELSET' at " + new Date());
			            if(set.isEmpty()){
                            dos.write("That`s not funny,do not use this action_type,unless u know what you are doing!!".getBytes());
                            System.out.println("several guys are using 'DELSET' to del data, we stoped this action!");
                            break;
                        }	
                        BlockingQueue<WriteTask> bq = sq.get(set);
                        if (bq != null) {
                            // 要删除这个集合,把在这个集合上进行写的线程停掉, null作为标志
                            bq.add(new WriteTask(null, null, null, 0, 0));
                        }
                        sp.delSet(set);
                        dos.write(1); // 返回一个字节1,代表删除成功
                        break;
                    }
                    case ActionType.SERVERINFO: {
                        ServerInfo si = new ServerInfo();
                        String str = "";
                        try {
                            str += si.getCpuTotalInfo()
                                    + System.getProperty("line.separator");
                            str += si.getMemInfo()
                                    + System.getProperty("line.separator");
                            for (String s : si.getDiskInfo())
                                str += s + System.getProperty("line.separator");
                        } catch (SigarException e) {
                            str = "#FAIL:" + e.getMessage();
                            e.printStackTrace();
                        }
                        dos.writeInt(str.length());
                        dos.write(str.getBytes());
                        break;
                    }
                    case ActionType.FEATURESEARCH: {
                        int feature_size = dis.readInt();
                        int obj_len = dis.readInt();
                        List<Feature> features = new ArrayList<Feature>(
                                feature_size);
                        ObjectInputStream ois = new ObjectInputStream(dis);
                        for (int i = 0; i < feature_size; i++) {
                            features.add((Feature) ois.readObject());
                        }
                        try {
                            BufferedImage bi = null;
                            if (obj_len > 0) {
                                byte[] obj = readBytes(obj_len, dis);
                                ByteArrayInputStream bais = new ByteArrayInputStream(
                                        obj, 0, obj_len);
                                bi = ImageIO.read(bais);
                            }
                            ResultSet rs = sp.featureSearch(bi, features);
                            if (rs != null && rs.getSize() > 0) {
                                dos.writeInt(1);
                                ObjectOutputStream oos = new ObjectOutputStream(dos);
                                oos.writeObject(rs);
                            } else
                                dos.writeInt(-1);
                        } catch (IOException e) {
                            e.printStackTrace();
                            dos.writeInt(-1);
                        }
                        break;
                    }
                    case ActionType.XSEARCH: {
                        int setlen = header[1] & 0xff;
                        int md5len = header[2] & 0xff;
                        String redirect = null;
                        if (setlen > 0 && md5len > 0) {
                            byte[] setmd5 = readBytes(setlen + md5len, dis);
                            String set = new String(setmd5, 0, setlen);
                            String md5 = new String(setmd5, setlen, md5len);
                            long offset = dis.readLong();
                            int length = dis.readInt();
                            ObjectContent oc = null;
                            try {
                                oc = sp.getPhoto(set, md5);
                            } catch (RedirectException e) {
                                // ok, this means we should notify the client to
                                // retry another server
                                redirect = e.info;
                            }
                            if (oc != null && oc.content != null) {
                                dos.writeInt(oc.length);
                                dos.write(oc.content, 0, oc.length);
                            } else {
                                if (redirect != null) {
                                    dos.writeInt(-1 * redirect.length());
                                    dos.write(redirect.getBytes());
                                    dos.writeLong(offset);
                                    dos.writeInt(length);
                                } else {
                                    dos.writeInt(-1);
                                }
                            }
                        } else {
                            dos.writeInt(-1);
                        }
                        break;
                    }
                    case ActionType.GETINFO: {
                        int setlen = header[1] & 0xff;
                        int md5len = header[2] & 0xff;
//                        String redirect = null;
                        if (setlen > 0 && md5len > 0) {
                            byte[] setmd5 = readBytes(setlen + md5len, dis);
                            String set = new String(setmd5, 0, setlen);
                            String md5 = new String(setmd5, setlen, md5len);
                            byte[] info = null;
                            info = sp.getFileInfo(set, md5);
                            if (info != null) {
                                dos.writeInt(info.length);
                                dos.write(info);
                            } else {
                                dos.writeInt(-1);
                            }
                        } else {
                            dos.writeInt(-1);
                        }
                        break;
                    }
                }
                dos.flush();
            }
        } catch (EOFException e) {
            // socket close, it is ok
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (sp != null)
                sp.close();
            try {
                System.out.println("client is closing -> " + s.getRemoteSocketAddress()
                        + ", thread name -> " + Thread.currentThread().getName());
                s.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 从输入流中读取count个字节
     *
     * @param count
     * @return
     */
    public byte[] readBytes(int count, InputStream istream) throws IOException {
        byte[] buf = threadLocalRecvBuffer.get();
        int n = 0;

        if (buf.length < count) {
            buf = new byte[count];
        }
        while (count > n) {
            n += istream.read(buf, n, count - n);
        }

        return buf;
    }

    public byte[] readBytesN(int count, InputStream istream) throws IOException {
        byte[] buf = new byte[count];
        int n = 0;

        while (count > n) {
            n += istream.read(buf, n, count - n);
        }

        return buf;
    }
}
