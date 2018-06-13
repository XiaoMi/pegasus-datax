package com.alibaba.datax.plugin.writer.pegasuswriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.charset.Charset;
import java.util.*;

/**
 * Created by qinzuoyan on 18/6/1.
 */
public class PegasusWriter extends Writer {
    public static class Job extends Writer.Job {
        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.validateParameter();
        }

        private void validateParameter() {
            // check cluster & table
            PegasusUtil.openTable(this.originalConfig);

            // check write_type
            String writeType = this.originalConfig.getString(Key.WRITE_TYPE, Constant.WRITE_TYPE_INSERT);
            if (!writeType.equals("insert") && !writeType.equals("delete")) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置了不合法的%s: [%s]", Key.WRITE_TYPE, writeType));
            }

            // check encoding
            String encoding = this.originalConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING);
            try {
                Charsets.toCharset(encoding);
            } catch (Exception e) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置了不合法的%s: [%s]", Key.ENCODING, encoding));
            }

            // check int values
            int timeout = this.originalConfig.getInt(Key.TIMEOUT_MS, Constant.DEFAULT_TIMEOUT_MS);
            if (timeout < 0) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置了不合法的%s: [%d]", Key.TIMEOUT_MS, timeout));
            }
            int ttl = this.originalConfig.getInt(Key.TTL_SECONDS, Constant.DEFAULT_TTL_SECONDS);
            if (ttl < 0) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置了不合法的%s: [%d]", Key.TTL_SECONDS, ttl));
            }
            int retry_count = this.originalConfig.getInt(Key.RETRY_COUNT, Constant.DEFAULT_RETRY_COUNT);
            if (retry_count < 0) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置了不合法的%s: [%d]", Key.RETRY_COUNT, retry_count));
            }
            int retry_delay = this.originalConfig.getInt(Key.RETRY_DELAY_MS, Constant.DEFAULT_RETRY_DELAY_MS);
            if (retry_delay < 0) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                        String.format("您配置了不合法的%s: [%d]", Key.RETRY_DELAY_MS, retry_delay));
            }

            // check mapping
            Configuration mappingConf = this.originalConfig.getConfiguration(Key.MAPPING);
            if (null == mappingConf) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.REQUIRED_VALUE,
                        String.format("您需要指定%s", Key.MAPPING));
            }

            mappingConf.getNecessaryValue(Key.HASH_KEY, PegasusWriterErrorCode.MAPPING_REQUIRED_VALUE);

            List<Configuration> values = mappingConf.getListConfiguration(Key.VALUES);
            if (null == values) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.MAPPING_REQUIRED_VALUE,
                        String.format("您需要在%s中指定%s", Key.MAPPING, Key.VALUES));
            }
            if (values.isEmpty()) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.MAPPING_REQUIRED_VALUE,
                        String.format("您需要在%s中指定非空的%s", Key.MAPPING, Key.VALUES));
            }

            Set<String> sortKeySet = new HashSet<String>();
            for (int i = 0; i < values.size(); i++) {
                Configuration valueConf = values.get(i);
                String sortKey = valueConf.getString(Key.SORT_KEY);
                if (null == sortKey) {
                    throw DataXException.asDataXException(PegasusWriterErrorCode.MAPPING_REQUIRED_VALUE,
                            String.format("您需要在%s[%d]中指定%s", Key.VALUES, i, Key.SORT_KEY));
                }
                if (sortKeySet.contains(sortKey)) {
                    throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您在%s中指定了重复的%s: %s", Key.VALUES, Key.SORT_KEY, sortKey));
                }
                sortKeySet.add(sortKey);
                String value = valueConf.getString(Key.VALUE);
                if (null == value && writeType.equals(Constant.WRITE_TYPE_INSERT)) {
                    throw DataXException.asDataXException(PegasusWriterErrorCode.MAPPING_REQUIRED_VALUE,
                            String.format("您需要在%s[%d]中指定%s", Key.VALUES, i, Key.VALUE));
                }
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> writerSplitConfigs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                writerSplitConfigs.add(this.originalConfig);
            }
            return writerSplitConfigs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;

        private boolean isInsert;

        private Charset encoding;

        private int timeoutMs;

        private int ttlSeconds;

        private int retryCount;

        private int retryDelayMs;

        private PegasusTableInterface pegasusTable;

        private String hashKey;

        private List<Pair<String, String>> sortKeyList;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            this.isInsert = this.writerSliceConfig.getString(Key.WRITE_TYPE, Constant.WRITE_TYPE_INSERT).equals(Constant.WRITE_TYPE_INSERT);
            this.encoding = Charsets.toCharset(this.writerSliceConfig.getString(Key.ENCODING, Constant.DEFAULT_ENCODING));
            this.timeoutMs = this.writerSliceConfig.getInt(Key.TIMEOUT_MS, Constant.DEFAULT_TIMEOUT_MS);
            this.ttlSeconds = this.writerSliceConfig.getInt(Key.TTL_SECONDS, Constant.DEFAULT_TTL_SECONDS);
            this.retryCount = this.writerSliceConfig.getInt(Key.RETRY_COUNT, Constant.DEFAULT_RETRY_COUNT);
            this.retryDelayMs = this.writerSliceConfig.getInt(Key.RETRY_DELAY_MS, Constant.DEFAULT_RETRY_DELAY_MS);
            this.pegasusTable = PegasusUtil.openTable(this.writerSliceConfig);
            Configuration mappingConf = this.writerSliceConfig.getConfiguration(Key.MAPPING);
            this.hashKey = mappingConf.getNecessaryValue(Key.HASH_KEY, PegasusWriterErrorCode.MAPPING_REQUIRED_VALUE);
            this.sortKeyList = new ArrayList<Pair<String, String>>();
            List<Configuration> values = mappingConf.getListConfiguration(Key.VALUES);
            for (Configuration valueConf : values) {
                String sortKey = valueConf.getString(Key.SORT_KEY);
                String value = valueConf.getString(Key.VALUE);
                this.sortKeyList.add(Pair.of(sortKey, value));
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            Map<String, String> subMap = new HashMap<String, String>();
            StrSubstitutor substitutor = new StrSubstitutor(subMap);
            List<Pair<byte[], byte[]>> insertValues = null;
            List<byte[]> deleteValues = null;
            while ((record = recordReceiver.getFromReader()) != null) {
                try {
                    int columnCount = record.getColumnNumber();
                    for (int i = 0; i < columnCount; i++) {
                        subMap.put(String.valueOf(i), PegasusUtil.columnToString(record.getColumn(i)));
                    }
                    byte[] hashKey = substitutor.replace(this.hashKey).getBytes(this.encoding);
                    if (this.isInsert) {
                        insertValues = new ArrayList<Pair<byte[], byte[]>>();
                        for (Pair<String, String> pair : this.sortKeyList) {
                            byte[] sortKey = substitutor.replace(pair.getKey()).getBytes(this.encoding);
                            byte[] value = substitutor.replace(pair.getValue()).getBytes(this.encoding);
                            insertValues.add(Pair.of(sortKey, value));
                        }
                    } else {
                        deleteValues = new ArrayList<byte[]>();
                        for (Pair<String, String> pair : this.sortKeyList) {
                            byte[] sortKey = substitutor.replace(pair.getKey()).getBytes(this.encoding);
                            deleteValues.add(sortKey);
                        }
                    }
                    int retry = 0;
                    while (true) {
                        try {
                            if (this.isInsert) {
                                this.pegasusTable.multiSet(hashKey, insertValues, this.ttlSeconds, this.timeoutMs);
                            } else {
                                this.pegasusTable.multiDel(hashKey, deleteValues, this.timeoutMs);
                            }
                            break;
                        } catch (PException pe) {
                            if (retry >= this.retryCount) {
                                throw DataXException.asDataXException(PegasusWriterErrorCode.RUNTIME_EXCEPTION,
                                        String.format("更新数据到Pegasus失败: ", pe.getMessage()));
                            }
                        }
                        if (this.retryDelayMs > 0) {
                            Thread.sleep(this.retryDelayMs);
                        }
                        retry++;
                    }
                } catch (Exception e) {
                    super.getTaskPluginCollector().collectDirtyRecord(record, e);
                }
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
