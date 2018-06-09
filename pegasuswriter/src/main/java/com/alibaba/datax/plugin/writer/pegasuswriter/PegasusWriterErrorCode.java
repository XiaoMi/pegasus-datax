package com.alibaba.datax.plugin.writer.pegasuswriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by qinzuoyan on 18/6/1.
 */
public enum PegasusWriterErrorCode implements ErrorCode {
    RUNTIME_EXCEPTION("PegasusWriter-00", "出现运行时异常,请联系我们."),
    REQUIRED_VALUE("PegasusWriter-01", "您在配置中缺失了必须填写的参数值."),
    MAPPING_REQUIRED_VALUE("HdfsWriter-02", "您在mapping配置中缺失了必须填写的参数值."),
    ILLEGAL_VALUE("PegasusWriter-03", "您在配置中填写的参数值不合法."),
    UNKNOWN_EXCEPTION("PegasusWriter-04","出现未知异常,请联系我们.");

    private final String code;
    private final String description;

    private PegasusWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
