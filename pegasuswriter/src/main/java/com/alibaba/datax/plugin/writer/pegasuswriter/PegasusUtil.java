package com.alibaba.datax.plugin.writer.pegasuswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.xiaomi.infra.pegasus.client.*;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by qinzuoyan on 18/6/1.
 */
public class PegasusUtil {
    private static Map<String, PegasusClientInterface> clientMap = new HashMap<String, PegasusClientInterface>();

    public static final String META_SERVERS_KEY = "meta_servers";

    public static PegasusTableInterface openTable(Configuration conf) {
        String cluster = conf.getNecessaryValue(Key.CLUSTER, PegasusWriterErrorCode.REQUIRED_VALUE);
        String tableName = conf.getNecessaryValue(Key.TABLE, PegasusWriterErrorCode.REQUIRED_VALUE);
        PegasusClientInterface client;
        synchronized (clientMap) {
            client = clientMap.get(cluster);
            if (client == null) {
                Properties prop = new Properties();
                prop.setProperty(META_SERVERS_KEY, cluster);
                try {
                    client = new PegasusClient(prop);
                } catch (PException e) {
                    throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                            "创建PegasusClient失败,请检查cluster配置: " + e.getMessage());
                }
                clientMap.put(cluster, client);
            }
        }
        PegasusTableInterface table;
        try {
            table = client.openTable(tableName);
        } catch (PException e) {
            throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                    "打开PegasusTable失败,请检查table配置: " + e.getMessage());
        }
        return table;
    }

    public static String columnToString(Column column) {
        String str = column.asString();
        return str == null ? "" : str;
    }
}