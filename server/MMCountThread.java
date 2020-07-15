package mammoth.server;


import mammoth.jclient.KeyFactory;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;


/**
 *
 * @Description: 只在一个节点启用本线程统计入库数
 * @author gaoxiaolin
 *
 */
public class MMCountThread implements Runnable{
	private double lastWn = 0;
	private double lastRn = 0;
	private long lastTs = System.currentTimeMillis();

	private ServerConf conf;
	private static final List<String> diskArrayBalance = new ArrayList<>();
	private volatile static boolean isSyn = false;
	public MMCountThread(ServerConf conf) {
		super();
		this.conf = conf;
	}
	public static List<String> getdDiskArrayBalance(){
		synchronized (diskArrayBalance) {
			return diskArrayBalance;
		}
	}
	public static boolean getIsSyn(){
		return isSyn;
	}

	/**
	 * double 四舍五入成Int
	 * @param dou
	 * @return
	 */
	public static int DoubleFormatInt(Double dou){
		DecimalFormat df = new DecimalFormat("######0"); //四色五入转换成整数
		return Integer.parseInt(df.format(dou));
	}
	/**
	 * 启用disk的balance模式时，计算每个磁盘需要分配的数据比例
	 * @return
	 */
	private void midBalance(){
		Map<String, Long> spaceDisk = new HashMap<>();

		Long minSpace = Long.MAX_VALUE;
		for (String d : conf.getStoreArray()) {
			File f = new File(d);
			minSpace = f.getUsableSpace() < minSpace ? f.getUsableSpace():minSpace;
			spaceDisk.put(d, f.getUsableSpace());
		}
		List<String> list = new ArrayList<>();
		for (Map.Entry<String, Long> entry : spaceDisk.entrySet()){
			int pro = DoubleFormatInt((double)entry.getValue()/(double)minSpace);
			for(int i = pro; i > 0; i --){
				list.add(entry.getKey());
			}
		}
		synchronized (diskArrayBalance){
			diskArrayBalance.clear();
			diskArrayBalance.addAll(list);
		}

	}


	private void bandWidthCount(){
		long cur = System.currentTimeMillis();
//		long curTime = cur/1000-cur/1000%10;
		long curTime = cur - cur%10000;

		double wn = ServerProfile.writtenBytes.longValue() / 1024.0;
		double rn = ServerProfile.readBytes.longValue() / 1024.0;
		double wbw = (wn - lastWn) / ((cur - lastTs) / 1000.0);
		double rbw = (rn - lastRn) / ((cur - lastTs) / 1000.0);
		lastTs = cur;
		lastWn = wn;
		lastRn = rn;
		//		long dnr = ServerProfile.readN.longValue();
		//		long dl = ServerProfile.readDelay.longValue();
		StorePhoto.bandWidthCount(curTime, (long)wbw);

	}

	
	@Override
	public void run() {
		// TODO Auto-generated method stub

		Timer timer = new Timer();
		//1.统计所有数据
//		if(conf.isSSMaster()){
//			TimerTask task1 = new TimerTask(){
//				@Override
//				public void run() {
//					StorePhoto.mmCount(conf);
//				}
//			};
//			timer.scheduleAtFixedRate(task1, 10, conf.getMasterCountTime());
//		}
		//2.统计本节点入库情况
		TimerTask task2 = new TimerTask(){
			@Override
			public void run() {
				StorePhoto.sumStoreNum(conf);
			}
		};
		timer.scheduleAtFixedRate(task2, 10, conf.getThisCountTime());
		//3.balance策略生成新的diskArray
		TimerTask task3 = new TimerTask() {
			@Override
			public void run() {
				midBalance();
			}
		};
		timer.scheduleAtFixedRate(task3, 100, conf.getBalanceDiskTime());
		//4.统计写入速度
//		TimerTask task4 = new TimerTask() {
//			@Override
//			public void run() {
//				bandWidthCount();
//			}
//		};
//		timer.scheduleAtFixedRate(task4, 100, 10000);
	}

}
