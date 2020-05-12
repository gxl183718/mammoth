package mammoth.server;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

/**
 * leveldb接口
 *
 * @author zzq
 */
public class RocksDBInterface {

    public static class Rocks {
        private int rocksid = 0;
        private DB db = null;
        private String db_path = "./leveldb";
        private Options options = null;

        public boolean open(String prefix) {
            boolean r = true;

            File f = new File(prefix + "/" + db_path);
            f.mkdirs();

            options = new Options().createIfMissing(true);
            //.setIncreaseParallelism(64 * 1024 * 1024);
            try {
                db = factory.open(new File(prefix + "/" + db_path), options);
                System.out.println("Rocks DB is starting");
            } catch (IOException e) {
                e.printStackTrace();
                r = false;
            }
            return r;
        }

        public void write(String key, String value) {
            try {
                db.put(key.getBytes(), value.getBytes());
            } catch (DBException e) {
                e.printStackTrace();
            }
        }

        public void delete(String key) {
            try {
                db.delete(key.getBytes());
            } catch (DBException e) {
                e.printStackTrace();
            }
        }

        public String read(String key) {
            try {
                byte[] vb = db.get(key.getBytes());
                if (vb != null)
                    return new String(vb);
            } catch (DBException e) {
                e.printStackTrace();
            }
            return null;
        }

        private void close() {
            if (db != null)
                try {
                    db.close();
//					options.dispose();
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }

        public Rocks(int rocksid) {
            this.rocksid = rocksid;
        }
    }



    private static ArrayList<Rocks> rockss = new ArrayList<Rocks>();

    private static boolean isOpenned = false;

    public static Rocks getRocks(int rocksid) {
        return rockss.get(rocksid);
    }

    public RocksDBInterface(ServerConf conf) {
        if (!isOpenned)
            for (int i = 0; i < conf.getRocks_prefix().size(); i ++){
                rockss.add(new Rocks(i));
                isOpenned = rockss.get(i).open(conf.getRocks_prefix().get(i));
            }
    }

    public void rocksClose() {
        if (isOpenned)
            for (int i = 0; i < rockss.size(); i ++){
                rockss.get(i).close();

            }
    }

}
