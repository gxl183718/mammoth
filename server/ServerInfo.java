package mammoth.server;

import org.hyperic.sigar.*;

import java.util.ArrayList;
import java.util.List;

public class ServerInfo {

    private Sigar sigar;

    public ServerInfo() {
        sigar = new Sigar();
//		System.
    }

    /**
     * 获取内存的总量，使用量(KB)和使用率
     *
     * @return
     * @throws SigarException
     */
    public String getMemInfo() throws SigarException {
        Mem mem = sigar.getMem();
        String s = "memory info(KB):total," + mem.getTotal() / 1024 + "\t used," + mem.getUsed() / 1024
                + "\t used percent," + String.format("%.1f", mem.getUsedPercent()) + "%";
        return s;
    }

    /**
     * 获取cpu总的使用率,多块cpu按100％来算，分别有用户使用率，系统使用率，等待率和总的使用率
     *
     * @return
     * @throws SigarException
     */
    public String getCpuTotalInfo() throws SigarException {
        CpuPerc[] cpuList = sigar.getCpuPercList();
        int n = cpuList.length;
        double tu = 0.0, ts = 0.0, tw = 0.0, tc = 0.0;
        for (CpuPerc cp : cpuList) {
            tc += cp.getCombined();
            tu += cp.getUser();
            ts += cp.getSys();
            tw += cp.getWait();
        }
        return "cpu info:total " + CpuPerc.format(tc / n) + "\t user,"
                + CpuPerc.format(tu / n) + "\t system,"
                + CpuPerc.format(ts / n) + "\t wait," + CpuPerc.format(tw / n);
    }

    /**
     * 获取每一块cpu的信息，结果放在list里
     *
     * @return
     */
    public List<String> getCpuListInfo() {
        return null;
    }

    /**
     * 获得每个磁盘的总量,使用量和使用率
     *
     * @return
     * @throws SigarException
     */
    public List<String> getDiskInfo() throws SigarException {
        FileSystem[] fslist = sigar.getFileSystemList();
        List<String> ls = new ArrayList<String>();
        for (FileSystem fs : fslist) {
//			if(fs.getType() == FileSystem.TYPE_LOCAL_DISK )
            {
                FileSystemUsage usage = sigar.getFileSystemUsage(fs.getDirName());
                String s = fs.getDirName() + ": total(KB)," + usage.getTotal()
                        + "\t used," + usage.getUsed()
                        + "\t used percent," + CpuPerc.format(usage.getUsePercent());
                ls.add(s);
            }
        }
        return ls;
    }
}
