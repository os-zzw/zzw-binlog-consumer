package zzw;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.google.common.collect.Maps;

import zzw.util.BinglogEventListener;
import zzw.util.BinlogCache;
import zzw.util.BinlogResolver;

/**
 * @author zhangzhewei
 */
public class BinlogTest {

    public static void main(String[] args) throws IOException {
        BinaryLogClient binaryLogClient = new BinaryLogClient("39.96.161.189", 3306, "zzw",
                "Zzw5680*");

        BinlogCache binlogCache = new BinlogCache("39.96.161.189", 3306, "zzw", "Zzw5680*", "joda");

        UserInviteBinlogResolver userInviteBinlogResolver = new UserInviteBinlogResolver();
        ExecutorService cachedThreadPool = Executors.newCachedThreadPool();

        Map<String, BinlogResolver> resolverMap = Maps.newHashMap();
        Map<BinlogResolver, ExecutorService> forceSingleThreadPools = Maps.newHashMap();
        resolverMap.put("userInvite", userInviteBinlogResolver);
        binaryLogClient.registerEventListener(new BinglogEventListener(binlogCache, resolverMap,
                cachedThreadPool, forceSingleThreadPools));
        binaryLogClient.connect();
    }



}
