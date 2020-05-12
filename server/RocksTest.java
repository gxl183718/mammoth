package mammoth.server;

import java.util.ArrayList;

public class RocksTest {
    public static void main(String[] args)  {
        ServerConf sc = null;
        try {
            sc = new ServerConf(111111);
        } catch (Exception e) {
            e.printStackTrace();
        }
        ArrayList list = new ArrayList();
        list.add("/mmd1");
        sc.setRocks_prefix(list);
        RocksDBInterface ri = new RocksDBInterface(sc);
        
	if("write".equals(args[0])){
            RocksDBInterface.getRocks(0).write(args[1], args[2]);
        }else if("read".equals(args[0])){
            RocksDBInterface.getRocks(0).read(args[1]);
        }
    }
}

