package com.apple.aml.stargate.common.options;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.event.Level;

import java.io.Serializable;

import static com.apple.aml.stargate.common.utils.LogUtils.parseLevel;
import static com.apple.jvm.commons.util.Strings.isBlank;
import static lombok.AccessLevel.NONE;

@Data
public class BaseOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    @Optional
    private String schemaId;
    @Optional
    private String schemaReference;
    @Optional
    private String avroConverter;
    @Optional
    private String logPayload; // logs only incoming payload
    @ToString.Exclude
    @Getter(NONE)
    @Setter(NONE)
    private Level logPayloadLevel;

    public void setLogPayload(final String logPayload) {
        if (isBlank(logPayload)) return;
        this.logPayload = logPayload;
        this.logPayloadLevel = parseLevel(this.logPayload);
    }

    public Level logPayloadLevel() {
        return logPayloadLevel;
    }
}
