package com.apple.aml.stargate.common.pojo;

import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class TxnStatus<O> implements Serializable {
    public static final int STATUS_CODE_SUCCESS = 0;
    public static final int STATUS_CODE_ERROR = -1;
    private static final long serialVersionUID = 1L;
    private final boolean status;
    private final int statusCode;
    private final String statusMessage;
    private final Map<String, O> debugInfo;

    public TxnStatus(final boolean status, final int statusCode, final String statusMessage, final Map<String, O> debugInfo) {
        this.status = status;
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.debugInfo = debugInfo;
    }

    @SuppressWarnings("unchecked")
    public static TxnStatus successStatus(final String statusMessage) {
        return new TxnStatus(true, STATUS_CODE_SUCCESS, statusMessage, null);
    }

    @SuppressWarnings("unchecked")
    public static TxnStatus txnStatus(final boolean status, final int statusCode, final String statusMessage) {
        return new TxnStatus(status, statusCode, statusMessage, null);
    }

    public static <O> TxnStatus<O> errorStatus(final int errorCode, final String errorMessage, final Map<String, O> debugInfo) {
        return new TxnStatus<O>(false, errorCode, errorMessage, debugInfo);
    }

    public static <O> TxnStatus<O> errorStatus(final String errorMessage, final Map<String, O> debugInfo) {
        return new TxnStatus<O>(false, STATUS_CODE_ERROR, errorMessage, debugInfo);
    }
}
