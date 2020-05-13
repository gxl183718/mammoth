package mammoth.common;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**redis连接池选择器**/
public class RedisPoolSelector {
	// Note-XXX: in STANDALONE mode, L2 pool is the same as L1 pool
	public class RedisConnection {
		public Jedis jedis;
		public RedisPool rp;
		// L1 or L2.x
		public String id;
	}
	/**ROUND_ROBIN, LOAD_BALANCE,*/
	public enum ALLOC_POLICY {
		ROUND_ROBIN, LOAD_BALANCE,
	}
	private ALLOC_POLICY type = ALLOC_POLICY.ROUND_ROBIN;
	private int lastIdx = -1;
	private MMConf conf;
	private RedisPool rpL1;
	private ConcurrentHashMap<String, RedisPool> rpL2 =
			new ConcurrentHashMap<String, RedisPool>();
	private TimeLimitedCacheMap cached = 
			new TimeLimitedCacheMap(270, 60, 300, TimeUnit.SECONDS);
	private boolean useCache = false;
	
	public RedisPoolSelector(MMConf conf, RedisPool rpL1) throws Exception {
		this.conf = conf;
		this.rpL1 = rpL1;
		Jedis jedis = rpL1.getResource();
		if (jedis == null)
			throw new Exception("get RedisPool from L1 failed: get null");

		setUseCache(conf.isRpsUseCache());

		switch (conf.getRedisMode()) {
		case SENTINEL:
			try {
				// connect to L1, get all L2 servers to construct a L2 pool
				Map<String, String> l2mn = jedis.hgetAll("mm.l2");

				for (Map.Entry<String, String> entry : l2mn.entrySet()) {
					// get master redis instance from sentinel by masterName
					RedisPool rp = new RedisPool(conf, entry.getValue());
					rp.setPid(entry.getKey());
					rpL2.putIfAbsent(entry.getKey(), rp);
				}
				// get client alloc policy
				String stype = jedis.get("mm.alloc.policy");
				if (stype == null || stype.equalsIgnoreCase("round_robin"))
					type = ALLOC_POLICY.ROUND_ROBIN;
				else if (stype.equalsIgnoreCase("load_balance"))
					type = ALLOC_POLICY.LOAD_BALANCE;
			} catch (JedisException je) {
				je.printStackTrace();
			} finally {
				rpL1.putInstance(jedis);
			}
			break;
		case STANDALONE:
			if (rpL1 != null)
				rpL2.putIfAbsent("0", rpL1);
			break;
		case CLUSTER:
			break;
		}
	}
	
	public void setRpL1(RedisPool rpL1) {
		this.rpL1 = rpL1;
	}
	
	public String lookupSet(String set) throws Exception {
		return __lookupSet(set);
	}
	
	public static class RException extends Exception{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		public RException(){
			super();
		}
		public RException(String e){
			super(e);
		}
	}
	
	private String __lookupSet(String set) throws RException  {
		String id = null;
		Jedis jedis = rpL1.getResource();
		if (jedis == null) 
			throw new RException("lookup from L1 faield: get null");
		
		try {
			id = jedis.get("`" + set);
			if (id != null && isUseCache()) {
				cached.put(set, id);
			}
		} finally {
			rpL1.putInstance(jedis);
		}
		
		return id;
	}
	
	private String __createSet(String set) throws RException {
		String id = null;
		Jedis jedis = rpL1.getResource();
		if (jedis == null)
			throw new RException("lookup from L1 failed: get null");
		
		try {
			switch (type) {
			default:
			case ROUND_ROBIN:
				ArrayList<String> ids = new ArrayList<String>();
				ids.addAll(rpL2.keySet());
				//第一次启动客户端创建的第一个set足够随机，避免客户端启停带来的元数据向rpL2.keySet()中靠前的节点堆积；
				if (lastIdx == -1){
					Random random = new Random();
					lastIdx = random.nextInt(ids.size());
				}
				if (lastIdx >= ids.size())
					lastIdx = 0;
				if (ids.size() > 0) {
					id = ids.get(lastIdx);
					lastIdx++;
				}
				break;
			case LOAD_BALANCE:
				Long min = Long.MAX_VALUE;
				for (Map.Entry<String, RedisPool> entry : rpL2.entrySet()) {
					if (entry.getValue().getBalanceTarget().get() < min) {
						min = entry.getValue().getBalanceTarget().get();
						id = entry.getKey();
					}
				}
				break;
			}
			if (id != null) {
				long r = jedis.setnx("`" + set, id);
				if (r == 0) {
					id = jedis.get("`" + set);
				}
				if (id != null)
					rpL2.get(id).incrAlloced();
				//create set rocksid
			}
		} finally {
			rpL1.putInstance(jedis);
		}
		
		return id;
	}
	
	private RedisPool __lookupL2(String id) throws RException {
		Jedis jedis = rpL1.getResource();
		if (jedis == null)
			throw new RException("lookup from L1 failed: get null");
		
		try (Jedis jedis1 = rpL1.getResource()){
			String masterName = jedis.hget("mm.l2", id);
			if (masterName != null) {
				RedisPool rp = new RedisPool(conf, masterName);
				rp.setPid(id);
				rpL2.putIfAbsent(id, rp);
			}
		} finally {
			rpL1.putInstance(jedis);
			jedis.close();
		}
		return rpL2.get(id);
	}
	
	private RedisConnection __getL2_standalone(String set, boolean doCreate) {
		RedisConnection rc = new RedisConnection();
		rc.rp = rpL2.get("0");
		rc.id = "0";
		if (rc.rp != null) {
			rc.jedis = rc.rp.getResource();
		}
		return rc;
	}
	
	public void __cleanup_cached(String set) {
		cached.remove(set);
	}

	private RedisConnection __getL2_sentinel(String set, boolean doCreate) throws RException {
		RedisConnection rc = new RedisConnection();
		String id = (String)cached.get(set);
		if (id == null) {
			// lookup from L1
			id = __lookupSet(set);
		}
		if (id == null && doCreate) {
			id = __createSet(set);
		}
		if (id != null) {
			RedisPool rp = rpL2.get(id);
			if (rp == null) {
				// lookup from L1 by id
				rp = __lookupL2(id);
			}
			rc.rp = rp;
			rc.id = id;
		}
		if (rc.rp != null) {
			rc.jedis = rc.rp.getResource();
		}
		return rc;
	}
	
	public RedisConnection getL2(String set, boolean doCreate) throws RException {
		switch (conf.getRedisMode()) {
		case SENTINEL:
			return __getL2_sentinel(set, doCreate);
		case STANDALONE:
			return __getL2_standalone(set, doCreate);
		case CLUSTER:
		}
		return null;
	}
	
	public RedisConnection getL2ByPid(String pid) throws Exception {
		RedisConnection rc = new RedisConnection();

		switch (conf.getRedisMode()) {
		case SENTINEL:
			if (pid != null) {
				RedisPool rp = rpL2.get(pid);
				if (rp == null) {
					rp = __lookupL2(pid);
				}
				rc.rp = rp;
				rc.id = pid;
			}
			break;
		case STANDALONE:
			rc.rp = rpL2.get("0");
			rc.id = "0";
			break;
		case CLUSTER:
			break;
		}
		if (rc.rp != null)
			rc.jedis = rc.rp.getResource();
		return rc;
	}

	public void putL2(RedisConnection rc) {
		if (rc != null && rc.jedis != null)
			rc.rp.putInstance(rc.jedis);
	}
	
	public void quit() {
		for (Map.Entry<String, RedisPool> entry : rpL2.entrySet()) {
			entry.getValue().quit();
		}
		rpL2.clear();
		cached.close();
	}
	
	public ConcurrentHashMap<String, RedisPool> getRpL2() {
		return rpL2;
	}

	public boolean isUseCache() {
		return useCache;
	}

	public void setUseCache(boolean useCache) {
		this.useCache = useCache;
	}


}
