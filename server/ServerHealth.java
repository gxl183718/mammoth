package mammoth.server;

import mammoth.common.RedisPool;
import mammoth.common.RedisPoolSelector.RedisConnection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.exceptions.JedisException;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;

/**
 * ServerHealth contains the following module:
 *
 * 0. monitor info memory used_memory to trigger auto SS swapOut;
 * 1. check and clean mm.dedup.info;
 * 2. check and fix under/over replicated objects;-->没找到。。。。。。。。。。。。
 * 3. auto clean block file if there are no reference on it;-->若已开启wapOut，要慎重开启此功能
 *
 *
 * @author macan
 * @update gxl
 *
 */
public class ServerHealth extends TimerTask {
	private static final Logger logger = LogManager.getLogger(ServerHealth.class.getName());
	private ServerConf conf;
	private long lastScrub = System.currentTimeMillis();
	private boolean isMigrating = false;
	private boolean isCleaningDI = false;
	private boolean doFetch = false;
	private static long CLEAN_ITER_BASE = 50000;
	private long cleanIter = CLEAN_ITER_BASE;
	private static boolean balance = false;

	static boolean getBalance(){
		return balance;
	}


	static class SetInfo {
		long usedBlocks;
		long totalBlocks;
		long usedLength;
		long totalLength;
		int siteNode;

		SetInfo() {
			usedBlocks = 0;
			totalBlocks = 0;
			usedLength = 0;
			totalLength = 0;
			siteNode = 0;
		}
	}
	static Map<String, SetInfo> setInfos = new HashMap<>();

	ServerHealth(ServerConf conf) {
		super();
		this.conf = conf;
	}


	/**
	 * get usedMemory (byte) from info memory
	 * @param info
	 * @return
	 */
	private long getRedisUsedMemory(String info) {
		if (info != null) {
			String lines[] = info.split("\r\n");
			if (lines.length >= 9) {
				String used_mem[] = lines[1].split(":");
				if (used_mem.length == 2 && used_mem[0].equalsIgnoreCase("used_memory")) {
					return Long.parseLong(used_mem[1]);
				}
			}
		}
		return 0L;
	}


	private int getRocksid(String set) {
		Jedis jedis = null;
		try {
			jedis = StorePhoto.getRpL1(conf).getResource();
			String rocksid = jedis.hget("rocksid", set);
			if (rocksid != null) {
				int dn = Integer.parseInt(rocksid);
				return dn;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			StorePhoto.getRpL1(conf).putInstance(jedis);
		}
		return 1;
	}


	@Override
	public void run() {
		try {
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			String s = df.format(new Date());
			// del method --> getL1UsedMemory, add method --> get mm.balance
			long cur = System.currentTimeMillis();
			Jedis jedis = null;
			try {
				jedis = StorePhoto.getRpL1(conf).getResource();
				if (jedis == null)
					System.out.println(s + ":" + Thread.currentThread().getName() + " get redis connection failed.");
				else {
//						String info = jedis.info("memory");
//						getRedisUsedMemory(info);
					balance = jedis.get("mm.server.balance") == null ? false:true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				StorePhoto.getRpL1(conf).putInstance(jedis);
			}
			doFetch = true;

//			if (conf.isSSMaster() && doFetch) {
//				doFetch = false;
//				for (Entry<String, RedisPool> entry : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
//					long l2UsedMemory = 0;
//					Jedis jedisSO = null;
//					try {
//						jedisSO = entry.getValue().getResource();
//						if (jedisSO != null) {
//							String info = jedisSO.info("memory");
//							l2UsedMemory = getRedisUsedMemory(info);
//						}
//						if (l2UsedMemory > conf.getMemorySize()) {
//							System.out.println(s + ": " + Thread.currentThread().getName() +
//									" detect l2 " + entry.getKey() + " metaData because  used_memory="
//									+ l2UsedMemory + " > configMemorySize=" + conf.getMemorySize());
//							 __do_clean(s);//删除 dedupinfo
//						}
//					} finally {
//						entry.getValue().putInstance(jedisSO);
//					}
//				}
//			}
			//
			if (cur - lastScrub >= conf.getSpaceOperationInterval()) {//30s
//				scrubSets();//启用swapOut之后，慎用此功能
				//gatherSpaceInfo(conf);
				lastScrub = cur;
			} else{
				Thread.sleep(10000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * clean mm.dedup.info
	 * @param s
	 * @throws Exception
	 */
	private void __do_clean(String s) throws Exception {
		if (!isCleaningDI) {
			isCleaningDI = true;
			// NOTE: user can set cleanDedupInfo arg(iter) here
			int err = cleanDedupInfo(s + " [cleanDedupInfo]", cleanIter);
			if (err < 0) {
				System.out.println(s + " clean dedupinfo failed w/ " + err);
			} else if (err == 0) {
				cleanIter += CLEAN_ITER_BASE;
				if (cleanIter > CLEAN_ITER_BASE * 20)
					cleanIter -= CLEAN_ITER_BASE;
				System.out.println(s + " clean dedupinfo ZERO, adjust iter to " + cleanIter);
			} else {
				System.out.println(s + " clean dedupinfo " + err + " entries.");
			}
			isCleaningDI = false;
		}
	}

	/**
	 * clean mm.dedup.info, 删除kdays之前的mm.dedup.info
	 * @param lh
	 * @param xiter
	 * @return
	 * @throws Exception
	 */
	private int cleanDedupInfo(String lh, long xiter) throws Exception {
		int kdays = conf.getDi_keep_days();
		int deleted = 0;
		long cday = System.currentTimeMillis() / 86400000 * 86400;
		long iter = 300000, j = 0, bTs;
		Jedis jedis = null;

		if (xiter > 0)
			iter = xiter;
		bTs = cday - (kdays * 86400);
		System.out.println(
				lh + " keep day time is " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(bTs * 1000)));

		for (Entry<String, RedisPool> rp : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
			jedis = rp.getValue().getResource();
			try {
				if (jedis == null) {
					System.out.println(lh + " get jedis connection failed.");
					deleted--;
				} else {
					Map<String, String> infos = new HashMap<String, String>();
					ScanParams sp = new ScanParams();
					sp.match("*");
					boolean isDone = false;
					String cursor = ScanParams.SCAN_POINTER_START;

					while (!isDone) {
						ScanResult<Entry<String, String>> r = jedis.hscan("mm.dedup.info", cursor, sp);
						for (Entry<String, String> entry : r.getResult()) {
							infos.put(entry.getKey(), entry.getValue());
						}
						cursor = r.getCursor();
						if (cursor.equalsIgnoreCase("0")) {
							isDone = true;
						}
						j++;
						if (j > iter)
							break;
					}

					if (infos != null && infos.size() > 0) {
						for (Entry<String, String> entry : infos.entrySet()) {
							String[] k = entry.getKey().split("@");
							long ts = -1;

							if (k.length == 2) {
								try {
									if (Character.isDigit(k[0].charAt(0))) {
										ts = Long.parseLong(k[0]);
									} else {
										ts = Long.parseLong(k[0].substring(1));
									}
								} catch (Exception e) {
									// ignore it
									if (conf.isVerbose(1))
										System.out.println("Ignore set '" + k[0] + "'.");
								}
							}
							if (ts >= 0 && ts < bTs) {
								jedis.hdel("mm.dedup.info", entry.getKey());
								deleted++;
							}
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				rp.getValue().putInstance(jedis);
			}
		}

		return deleted;
	}




	/**
	 * 按时间获取前10%set
	 *
	 * @return
	 */
	private List<String> getSetsDoDel(Set<String> temp){
		List<String> listDel = new ArrayList<>();
		List<String> list = new ArrayList<>();
		list.addAll(temp);
		Collections.sort(list, Comparator.comparing(s -> s.substring(1)));
		for(int i = 0; i < list.size()/10; i ++){
			listDel.add(list.get(i));
		}
		return  listDel;
	}





	/**
	 * total and usable space of each disk
	 * @param conf
	 * @return
	 * @throws Exception
	 */
	public int gatherSpaceInfo(ServerConf conf) throws Exception {
		Jedis jedis = null;
		int err = 0;

		try {
			jedis = StorePhoto.getRpL1(conf).getResource();
			if (jedis == null)
				return -1;
			Pipeline p = jedis.pipelined();

			for (String d : conf.getStoreArray()) {
				File f = new File(d);
				if (err == 0) {
					p.hset("mm.space", ServerConf.serverId + "|" + d + "|T", "" + f.getTotalSpace());
					p.hset("mm.space", ServerConf.serverId + "|" + d + "|F", "" + f.getUsableSpace());
					p.hset("mm.space.usable", ServerConf.serverId + "|" + d,
							(double)f.getUsableSpace()/(double)f.getTotalSpace() + "");
				}
			}
			p.sync();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			StorePhoto.getRpL1(conf).putInstance(jedis);
		}

		return err;
	}



	/**
	 * scrub set --> if there is no reference in metaData,del this block in disk.
	 * TODO:功能停用，若启用务必考虑慢查询带来的性能影响
	 * @return
	 * @throws Exception
	 */
	/*private int scrubSets() throws Exception {
		TreeSet<String> sets = new TreeSet<String>();
		int err = 0;

		// get all sets
		for (Entry<String, RedisPool> entry : StorePhoto.getRPS(conf).getRpL2().entrySet()) {
			Jedis jedis = entry.getValue().getResource();
			if (jedis != null) {
				try {
					Set<String> keys = jedis.keys("*.blk.*");

					if (keys != null && keys.size() > 0) {
						for (String k : keys) {
							String set = k.split("\\.")[0];

							sets.add(set);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
					sets.clear();
				} finally {
					entry.getValue().putInstance(jedis);
				}
			}
		}

		// scrub for each set
		for (String set : sets) {
			err = scrubSetData(set);
		}

		return err;
	}*/

	/**
	 * scrub set
	 * @param set
	 * @return
	 * @throws Exception
	 */
	private int scrubSetData(String set) throws Exception {
		RedisConnection rc = null;
		int err = 0;

		try {
			rc = StorePhoto.getRPS(conf).getL2(set, false);
			if (rc.rp == null || rc.jedis == null) {
				System.out.println("get L2 pool " + rc.id + " : conn failed for " + set + " scrub.");
			} else {
				Jedis jedis = rc.jedis;
				// Map: disk.block -> refcount
				Map<String, BlockRef> br = new HashMap<>();
				Map<String, Long> blockId = new HashMap<>();
				Set<String> disks = new TreeSet<String>();
				ScanParams sp = new ScanParams();
				boolean isDone = false;
				String cursor = ScanParams.SCAN_POINTER_START;

				// BUG-XXX: if we get _blk max after hscan, then we might delete
				// a block
				// file false positive.
				for (String d : conf.getStoreArray()) {
					String _blk = jedis.hget("blk." + set, ServerConf.serverId + "." + d);
					if (_blk != null)
						blockId.put(d, Long.parseLong(_blk));
				}

				sp.match("*");
				while (!isDone) {
					ScanResult<Entry<String, String>> r = jedis.hscan(set, cursor, sp);

					for (Entry<String, String> entry : r.getResult()) {
						// parse value into hashmap
						String[] infos = entry.getValue().split("#");

						if (infos != null) {
							for (int i = 0; i < infos.length; i++) {
								String[] vf = infos[i].split("@");
								String disk = null, block = null, serverId = null;
								BlockRef ref = null;
								long len = 0;

								if (vf != null) {
									for (int j = 0; j < vf.length; j++) {
										switch (j) {
											case 1:
												break;
											case 2:
												// serverId
												serverId = vf[j];
												break;
											case 3:
												// blockId
												block = vf[j];
												break;
											case 5:
												// length
												try {
													len = Long.parseLong(vf[j]);
												} catch (Exception nfe) {
												}
												break;
											case 6:
												// diskId
												disk = vf[j];
												break;
										}
									}
									try {
										if (serverId != null && disk != null && block != null
												&& Long.parseLong(serverId) == ServerConf.serverId) {
											ref = br.get(disk + "." + block);
											if (ref == null) {
												ref = new BlockRef();
											}
											ref.updateRef(1, len);
											br.put(disk + "." + block, ref);
											if (!disks.contains(disk))
												disks.add(disk);
										}
									} catch (Exception nfe) {
									}
								}
							}
						}
					}
					cursor = r.getCursor();
					if (cursor.equalsIgnoreCase("0")) {
						isDone = true;
					}
				}

				if (!conf.getStoreArray().containsAll(disks)) {
					System.out.println("Got disks set isn't filled in configed store array (" + disks + " vs "
							+ conf.getStoreArray() + "), retain it!");
					disks.retainAll(conf.getStoreArray());
				}
				// if disks is empty set, we know that all refs are freed, thus
				// we can
				// put StoreArray to disks to free block files
				if (disks.isEmpty())
					disks.addAll(conf.getStoreArray());
				for (String d : disks) {
					long blockMax = 0;
					long usedBlocks = 0;
					long usedLength = 0, totalLength = 0;

					// find the max block id for this disk
					if (blockId.containsKey(d))
						blockMax = blockId.get(d);
					for (long i = 0; i <= blockMax; i++) {
						if (br.get(d + "." + i) == null) {
							if (fexist(d + "/" + conf.destRoot + set + "/b" + i)) {
								if (i < blockMax) {
									//if (conf.isVerbose(2))
									System.out.println("Clean block file b" + i + " in disk [" + d + "] set [" + set
											+ "] ...");
									delFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								} else {
									totalLength += statFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								}
							}
						} else {
							usedBlocks++;
							if (fexist(d + "/" + conf.destRoot + set + "/b" + i)) {
								totalLength += statFile(new File(d + "/" + conf.destRoot + set + "/b" + i));
								usedLength += br.get(d + "." + i).len;
								if (conf.isVerbose(3))
									System.out.println("Used  block file b" + i + " in disk [" + d + "] set [" + set
											+ "], ref=" + br.get(d + "." + i).ref);
							} else {
								System.out.println("Set [" + String.format("%8s", set) + "] in disk [" + d
										+ "] used but not exist(blk=" + i + ",ref=" + br.get(d + "." + i).ref + ").");
							}
						}
					}
					SetInfo si = setInfos.get(set);
					if (si == null) {
						si = new SetInfo();
					}
					si.usedBlocks = usedBlocks;
					si.totalBlocks = blockMax + 1;
					si.usedLength = usedLength;
					si.totalLength = totalLength;
					si.siteNode = Integer.parseInt(rc.id);
					setInfos.put(set, si);
					if (conf.isVerbose(1))
						System.out.println("Set [" + String.format("%8s", set) + "] in disk [" + d + "] u/t="
								+ usedBlocks + "/" + (blockMax + 1) + " B.UR="
								+ String.format("%.4f", (blockMax == 0 ? 0 : (double) usedBlocks / blockMax)) + " P.UR="
								+ String.format("%.4f", (totalLength == 0 ? 0 : (double) usedLength / totalLength)));
				}
			}
		} catch (JedisException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			StorePhoto.getRPS(conf).putL2(rc);
		}

		return err;
	}

	private class BlockRef {
		public long ref;
		public long len;

		public BlockRef() {
			ref = 0;
			len = 0;
		}

		public void updateRef(long refDelta, long lenDelta) {
			ref += refDelta;
			len += lenDelta;
		}
	}

	private boolean fexist(String fpath) {
		return new File(fpath).exists();
	}

	private long statFile(File f) {
		if (!f.exists())
			return 0;
		if (f.isFile())
			return f.length();

		return 0;
	}

	private void delFile(File f) {
		if (!f.exists())
			return;
		if (f.isFile())
			f.delete();
		else {
			for (File a : f.listFiles())
				if (a.isFile())
					a.delete();
				else
					delFile(a);
			f.delete();
		}
	}

}
