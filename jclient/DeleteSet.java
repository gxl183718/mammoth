package mammoth.jclient;

import mammoth.common.RedisPoolSelector.RedisConnection;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import java.io.*;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class DeleteSet {
    private ClientAPI ca;

    public DeleteSet(ClientAPI ca) {
        this.ca = ca;
    }

    //TODO:此功能停用，若启用务请必考虑慢查询带来的性能损耗，另获取全部sets逻辑错误--应为keys("`*");
    /*public void recycleSet(String dstr) throws ParseException, Exception {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        Date d = df.parse(dstr);
        Jedis jedis = ca.getPc().getRpL1().getResource();
        if (jedis == null) {
            throw new Exception("Cound not get avaliable Jedis instance.");
        }

        // get all sets
        TreeSet<String> temp = new TreeSet<String>();
        try {

            Set<String> keys = jedis.keys("*.blk.*");

            if (keys != null && keys.size() > 0) {
                String[] keya = keys.toArray(new String[0]);

                for (int i = 0; i < keya.length; i++) {
                    String set = keya[i].split("\\.")[0];

                    if (Character.isDigit(set.charAt(0))) {
                        temp.add(set);
                    } else {
                        temp.add(set.substring(1));
                    }
                }
            }

            String[] prefixs = new String[]{"", "i", "t", "a", "v", "o", "s"};
            for (String set : temp) {
                try {
                    long thistime = Long.parseLong(set);
                    if (d.getTime() > thistime * 1000) {
                        // ok, delete it
                        System.out.println("Del Set:\t" + set + "\t" + df.format(new Date(thistime * 1000)));

                        for (String prefix : prefixs) {
                            try {
                                delSet(prefix + set);
                            } catch (Exception e) {
                                System.out.println("DEE: on set " + set);
                            }
                        }
                    }
                } catch (NumberFormatException nfe) {
                    System.out.println("NFE: on set " + set);
                }
            }
        } catch (JedisException e) {
            System.out.println("recycleSet: Jedis exception: " + e.getMessage());
            temp = null;
        } catch (Exception e) {
            System.out.println("recycleSet: Exception: " + e.getMessage());
            temp = null;
        } finally {
            ca.getPc().getRpL1().putInstance(jedis);
        }
    }*/

    public void delSet(String set) throws Exception {
        RedisConnection rc = null;
        Jedis l1jedis = null;

        try {
            rc = ca.getPc().getRPS().getL2(set, false);
            if (rc.rp == null || rc.jedis == null) {
                throw new Exception("get L2 pool " + rc.id + " for set " + set + " failed");
            }
            Jedis jedis = rc.jedis;
            // 向所有拥有该集合的server发送删除请求,删除节点上的文件
            Iterator<String> ir = jedis.smembers(set + ".srvs").iterator();
            String info;
            while (ir.hasNext()) {
                info = ir.next();
                String[] infos = info.split(":");
                if (infos.length != 2) {
                    System.out.println("invalid format addr:" + info);
                    continue;
                }
                Socket s = new Socket();
                try {
                    s.setSoTimeout(60000);
                    s.connect(new InetSocketAddress(infos[0], Integer.parseInt(infos[1])));

                    byte[] bytes = new byte[2];
                    bytes[0] = (byte) ClientConf.userName.length();
                    bytes[1] = (byte) ClientConf.passWord.length();
                    s.getOutputStream().write(bytes);
                    s.getOutputStream().write((ClientConf.userName + ClientConf.passWord).getBytes());
                    s.getOutputStream().flush();
                    DataInputStream dis = new DataInputStream(s.getInputStream());
                    int num = dis.readInt();
                    if (num == -1) System.out.println("认证失败，客户端与服务端版本不一致！！");
                    else System.out.println("认证成功！");

                    byte[] header = new byte[4];
                    header[0] = ActionType.DELSET;
                    header[1] = (byte) set.length();
                    OutputStream os = s.getOutputStream();
                    InputStream is = s.getInputStream();
                    os.write(header);
                    os.write(set.getBytes());
                    if (is.read() != 1)
                        System.out.println(s.getRemoteSocketAddress() + "，删除时出现异常");
                    else
                        System.out.println(s.getRemoteSocketAddress() + "，删除成功");
                    s.close();
                } catch (SocketTimeoutException e) {
                    e.printStackTrace();
                    System.out.println(s.getRemoteSocketAddress() + "无响应");
                    return;
                } catch (ConnectException e) {
                    e.printStackTrace();
                    System.out.println("删除出现错误: " + infos[0] + " 拒绝连接");
                    return;
                } catch (IOException e) {
                    e.printStackTrace();
                    return;
                }
            }

            // 删除集合所在节点,集合对应块号,都在数据库0
            Iterator<String> ikeys1 = jedis.keys(set + ".*").iterator();
            Pipeline pipeline1 = jedis.pipelined();
            while (ikeys1.hasNext()) {
                String key1 = ikeys1.next();
                pipeline1.del(key1);
            }
            pipeline1.sync();

            // 删除集合
            jedis.del(set);

            // Delete L1 resource
            l1jedis = ca.getPc().getRpL1().getResource();
            l1jedis.del("`" + set);
            
            if (ca.getPc().getConf().isDataSync())
            	l1jedis.hincrBy("ds.del", set, 1);
        } catch (JedisException je) {
            System.out.println("Jedis exception: " + je.getMessage());
        } finally {
            ca.getPc().getRPS().putL2(rc);
            ca.getPc().getRpL1().putInstance(l1jedis);
        }
        System.out.println("删除集合'" + set + "'完成");
    }

    /**
     * 查询所有的active的节点的硬件信息，查找还有心跳信息的server
     * 每个节点的信息作为list中的一个元素
     *
     * @throws Exception
     */
    public List<String> getAllServerInfo() throws Exception {
        Jedis jedis = ca.getPc().getRpL1().getResource();
        if (jedis == null) {
            throw new Exception("Cound not get avaliable Jedis instance.");
        }
        List<String> ls = new ArrayList<>();

        try {
            Set<String> allNodes = jedis.zrange("mm.active", 0, -1);
            for (String node : allNodes) {
                if (jedis.exists("mm.hb." + node)){
                    String[] hostport = node.split(":");
                    ls.add(getServerInfo(hostport[0], Integer.parseInt(hostport[1])));
                }
            }
        } catch (JedisException je) {
            System.out.println("Jedis exception: " + je.getMessage());
        } catch (Exception e) {
            System.out.println("Get mm.hb.* failed: " + e.getMessage());
        } finally {
            ca.getPc().getRpL1().putInstance(jedis);
        }

        return ls;
    }

    /**
     * 查询某个节点的信息，指明节点名和端口号
     */
    public String getServerInfo(String name, int port) {
        Socket s = new Socket();
        String temp = "";
        try {
            s.setSoTimeout(10000);
            s.connect(new InetSocketAddress(name, port));
            temp += s.getRemoteSocketAddress() + "的信息:" + System.getProperty("line.separator");
            byte[] header = new byte[4];
            header[0] = ActionType.SERVERINFO;
            DataOutputStream dos = new DataOutputStream(s.getOutputStream());
            DataInputStream dis = new DataInputStream(s.getInputStream());
            dos.write(header);
            dos.flush();
            int n = dis.readInt();
            temp += new String(readBytes(n, dis));
//			System.out.println(s.getRemoteSocketAddress()+"，信息获取成功");
            s.close();
        } catch (SocketTimeoutException e) {
            e.printStackTrace();
            temp = s.getRemoteSocketAddress() + e.getMessage();
        } catch (ConnectException e) {
            e.printStackTrace();
            temp = name + ":" + e.getMessage();
        } catch (IOException e) {
            e.printStackTrace();
            temp = name + ":" + e.getMessage();
        }
        return temp;
    }

    /**
     * 从输入流中读取count个字节
     *
     * @param count
     * @return
     */
    public byte[] readBytes(int count, InputStream istream) {
        byte[] buf = new byte[count];
        int n = 0;
        try {
            while (count > n) {
                n += istream.read(buf, n, count - n);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return buf;
    }
}
