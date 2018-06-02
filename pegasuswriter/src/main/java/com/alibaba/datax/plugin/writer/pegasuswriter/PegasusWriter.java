package com.alibaba.datax.plugin.writer.pegasuswriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by qinzuoyan on 18/6/1.
 */
public class PegasusWriter extends Writer {
    public static class Job extends Writer.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            this.validateParameter();
        }

        private void validateParameter() {
            // check cluster & table
            PegasusUtil.openTable(this.originalConfig);

            // check columns
            List<Configuration> columns = this.originalConfig.getListConfiguration(Key.COLUMN);
            if (null == columns || columns.size() == 0) {
                throw DataXException.asDataXException(PegasusWriterErrorCode.REQUIRED_VALUE, "您需要指定column");
            }else{
                Set<String> nameSet = new HashSet<String>();
                for (Configuration eachColumnConf : columns) {
                    String name = eachColumnConf.getNecessaryValue(Key.NAME, PegasusWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    if (nameSet.contains(name)) {
                        throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                                String.format("您在columns中指定了name为[%s]的重复项", name));
                    }
                    nameSet.add(name);
                    eachColumnConf.getNecessaryValue(Key.INDEX, PegasusWriterErrorCode.COLUMN_REQUIRED_VALUE);
                    int index = eachColumnConf.getInt(Key.INDEX);
                    if (index < 0) {
                        throw DataXException.asDataXException(PegasusWriterErrorCode.ILLEGAL_VALUE,
                                String.format("您在columns中指定了name为[%s]的项，其index为负数", name));
                    }
                }
                if (!nameSet.contains(Key.HASH_KEY)) {
                    throw DataXException.asDataXException(PegasusWriterErrorCode.REQUIRED_VALUE,
                            String.format("您需要在columns中指定name为[%s]的项", Key.HASH_KEY));
                }
                if (nameSet.size() < 2) {
                    throw DataXException.asDataXException(PegasusWriterErrorCode.REQUIRED_VALUE,
                            String.format("您需要在columns中指定至少一个name不为[%s]的项", Key.HASH_KEY));
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
        private static final Logger LOG = LoggerFactory.getLogger(Task.class);

        private Configuration writerSliceConfig;

        private int timeoutMs; // default 10000

        private int ttlSeconds; // default 0

        private int retryCount; // default 2

        private int retryDelayMs; // default 10000

        private PegasusTableInterface pegasusTable;

        private int hashKeyIndex;

        private List<Pair<byte[], Integer>> sortKeyList;

        @Override
        public void init() {
            this.writerSliceConfig = getPluginJobConf();
            this.timeoutMs = this.writerSliceConfig.getInt(Key.TIMEOUT_MS, 10000);
            this.ttlSeconds = this.writerSliceConfig.getInt(Key.TTL_SECONDS, 0);
            this.retryCount = this.writerSliceConfig.getInt(Key.RETRY_COUNT, 2);
            this.retryDelayMs = this.writerSliceConfig.getInt(Key.RETRY_DELAY_MS, 10000);
            this.pegasusTable = PegasusUtil.openTable(this.writerSliceConfig);
            this.sortKeyList = new ArrayList<Pair<byte[], Integer>>();
            List<Configuration> columns = this.writerSliceConfig.getListConfiguration(Key.COLUMN);
            for (Configuration eachColumnConf : columns) {
                String name = eachColumnConf.getString(Key.NAME);
                int index = eachColumnConf.getInt(Key.INDEX);
                if (name.equals(Key.HASH_KEY)) {
                    this.hashKeyIndex = index;
                } else {
                    this.sortKeyList.add(Pair.of(name.getBytes(), Integer.valueOf(index)));
                }
            }
        }

        @Override
        public void prepare() {
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            Record record;
            while ((record = recordReceiver.getFromReader()) != null) {
                try {
                    Column column = record.getColumn(this.hashKeyIndex);
                     if (column == null) {
                         throw DataXException.asDataXException(PegasusWriterErrorCode.RUNTIME_EXCEPTION,
                                 String.format("record中找不到index为[%d]的column.", this.hashKeyIndex));
                     }
                     byte[] hashKey = PegasusUtil.columnToBytes(column);
                     List<Pair<byte[], byte[]>> values = new ArrayList<Pair<byte[], byte[]>>();
                     for (Pair<byte[], Integer> pair : this.sortKeyList) {
                         column = record.getColumn(pair.getRight());
                         if (column == null) {
                             throw DataXException.asDataXException(PegasusWriterErrorCode.RUNTIME_EXCEPTION,
                                     String.format("record中找不到index为[%d]的column.", pair.getRight()));
                         }
                         values.add(Pair.of(pair.getLeft(), PegasusUtil.columnToBytes(column)));
                     }
                     int retry = 0;
                     while (true) {
                         try {
                             this.pegasusTable.multiSet(hashKey, values, this.ttlSeconds, this.timeoutMs);
                             break;
                         } catch (PException pe) {
                             if (retry >= this.retryCount) {
                                 throw DataXException.asDataXException(PegasusWriterErrorCode.RUNTIME_EXCEPTION,
                                         String.format("写入数据到Pegasus失败: ", pe.getMessage()));
                             }
                         }
                         retry++;
                         Thread.sleep(this.retryDelayMs);
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
