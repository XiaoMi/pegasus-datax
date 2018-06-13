package com.alibaba.datax.plugin.writer.pegasuswriter;

/**
 * Created by qinzuoyan on 18/6/1.
 */
public class Constant {
    public static final String WRITE_TYPE_INSERT = "insert";
    public static final String WRITE_TYPE_DELETE = "delete";

    public static final String DEFAULT_ENCODING = "UTF-8";

    public static final int DEFAULT_TIMEOUT_MS = 10000;

    public static final int DEFAULT_TTL_SECONDS = 0; // means no ttl

    public static final int DEFAULT_RETRY_COUNT = 2;

    public static final int DEFAULT_RETRY_DELAY_MS = 10000;

    public static final String HASH_KEY = "_hash_key_";

    public static final String EMPTY_SORT_KEY = "_empty_sort_key_";

    public static final byte[] EMPTY_BYTES = "".getBytes();
}
