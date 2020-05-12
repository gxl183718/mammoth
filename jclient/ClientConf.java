package mammoth.jclient;

import mammoth.common.MMConf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Set;


public class ClientConf extends MMConf {

    private String serverName;
    private int serverPort;
    private int dupNum;            //一个文件存储份数
    private int sockPerServer;
    private boolean autoConf = false;
    private boolean logDupInfo = true;
    private int mgetTimeout = 60 * 1000;
    private int redisTimeout = 30 * 1000;
    private int serverRefreshInterval = 5 * 1000;
    private boolean printServerRefresh = false;
    
    private boolean dataSync = false;
    
    private Map<String, String> fileTypeMap;
    
    public Map<String, String> getFileTypeMap() {
	return fileTypeMap;
    }

    public void setFileTypeMap(Map<String, String> fileTypeMap) {
	this.fileTypeMap = fileTypeMap;
    }
    private int blen = 100;

    public int getVlen() {
        return vlen;
    }

    public void setVlen(int vlen) {
        this.vlen = vlen;
    }

    public int vlen = 7;

    public static String userName = "testName";
    public static String passWord = "testPwd";
    public static enum MODE {
        DEDUP, NODEDUP,
    }

    private MODE mode;

    private boolean getkeys_do_sort = true;

    public ClientConf(String serverName, int serverPort, String redisHost, int redisPort, MODE mode,
                      int dupNum) throws UnknownHostException {
        if (serverName != null)
            this.setServerName(serverName);
        else
            this.setServerName(InetAddress.getLocalHost().getHostName());
        this.setServerPort(serverPort);
        if (redisHost == null) {
            throw new UnknownHostException("Invalid redis server host name.");
        }
        this.mode = mode;

        this.dupNum = dupNum;
        this.setSockPerServer(5);
        this.setRedisMode(RedisMode.STANDALONE);
    }

    public ClientConf(Set<String> sentinels, MODE mode, int dupNum) throws UnknownHostException {
        this.dupNum = dupNum;
        this.mode = mode;
        this.setSockPerServer(5);
        this.setSentinels(sentinels);
        this.setRedisMode(RedisMode.SENTINEL);
    }

    public ClientConf() {
        //客户端redis L1-->L2映射，开启缓存
        setRpsUseCache(true);
        this.dupNum = 1;
        this.mode = MODE.NODEDUP;
        this.setRedisMode(RedisMode.STANDALONE);
        this.setSockPerServer(5);
        this.setAutoConf(true);
    }

    public String getServerName() {
        return serverName;
    }

    public void setServerName(String serverName) {
        this.serverName = serverName;
    }

    public int getServerPort() {
        return serverPort;
    }

    public void setServerPort(int serverPort) {
        this.serverPort = serverPort;
    }

    public MODE getMode() {
        return mode;
    }

    public void setMode(MODE mode) {
        this.mode = mode;
    }

    public int getDupNum() {
        return this.dupNum;
    }

    public void setDupNum(int dupNum) {
        this.dupNum = dupNum;
    }

    public int getSockPerServer() {
        return sockPerServer;
    }

    public void setSockPerServer(int sockPerServer) {
        this.sockPerServer = sockPerServer;
    }

    public boolean isAutoConf() {
        return autoConf;
    }

    public void setAutoConf(boolean autoConf) {
        this.autoConf = autoConf;
    }

    public boolean isGetkeys_do_sort() {
        return getkeys_do_sort;
    }

    public void setGetkeys_do_sort(boolean getkeys_do_sort) {
        this.getkeys_do_sort = getkeys_do_sort;
    }

    public boolean isLogDupInfo() {
        return logDupInfo;
    }

    public void setLogDupInfo(boolean logDupInfo) {
        this.logDupInfo = logDupInfo;
    }

    public int getMgetTimeout() {
        return mgetTimeout;
    }

    public void setMgetTimeout(int mgetTimeout) {
        this.mgetTimeout = mgetTimeout;
    }

    public int getRedisTimeout() {
        return redisTimeout;
    }

    public void setRedisTimeout(int redisTimeout) {
        this.redisTimeout = redisTimeout;
    }

    public boolean isPrintServerRefresh() {
        return printServerRefresh;
    }

    public void setPrintServerRefresh(boolean printServerRefresh) {
        this.printServerRefresh = printServerRefresh;
    }

    public int getServerRefreshInterval() {
        return serverRefreshInterval;
    }

    public void setServerRefreshInterval(int serverRefreshInterval) {
        this.serverRefreshInterval = serverRefreshInterval;
    }

    public int getBlen() {
        return blen * 1024 * 1024;
    }

    public void setBlen(int blen) {
        this.blen = blen;
    }

	public boolean isDataSync() {
		return dataSync;
	}

	public void setDataSync(boolean dataSync) {
		this.dataSync = dataSync;
	}
    
}
