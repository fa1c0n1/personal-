package com.apple.aml.stargate.common.configs;

import com.typesafe.config.Optional;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

import static com.apple.jvm.commons.util.Strings.isBlank;

@Data
public class NodeConfig implements Serializable {
    public static final String CONFIG_NAME = "stargate.nodeType.config";
    private static final long serialVersionUID = 1L;
    @Optional
    private String readerClassName;
    @Optional
    private String flinkReaderClassName;
    @Optional
    private String writerClassName;
    @Optional
    private String flinkWriterClassName;
    @Optional
    private String transformerClassName;
    @Optional
    private String flinkTransformerClassName;
    @Optional
    private Class<?> readerClass;
    @Optional
    private Class<?> flinkReaderClass;
    @Optional
    private Class<?> writerClass;
    @Optional
    private Class<?> flinkWriterClass;
    @Optional
    private Class<?> transformerClass;
    @Optional
    private Class<?> flinkTransformerClass;
    @Optional
    private Map<String, Object> readerDefaults;
    @Optional
    private Map<String, Object> writerDefaults;
    @Optional
    private Map<String, Object> transformerDefaults;
    @Optional
    protected String optionsClassName;
    @Optional
    protected Class<?> optionsClass;
    @Optional
    protected Set<String> aliases;

    public void init() throws Exception {
        if (this.readerClass == null && !isBlank(this.readerClassName))
            this.readerClass = Class.forName(this.readerClassName);
        if (this.writerClass == null && !isBlank(this.writerClassName))
            this.writerClass = Class.forName(this.writerClassName);
        if (this.transformerClass == null && !isBlank(this.transformerClassName))
            this.transformerClass = Class.forName(this.transformerClassName);
        if (this.flinkReaderClass == null && !isBlank(this.flinkReaderClassName))
            this.flinkReaderClass = Class.forName(this.flinkReaderClassName);
        if (this.flinkWriterClass == null && !isBlank(this.flinkWriterClassName))
            this.flinkWriterClass = Class.forName(this.flinkWriterClassName);
        if (this.flinkTransformerClass == null && !isBlank(this.flinkTransformerClassName))
            this.flinkTransformerClass = Class.forName(this.flinkTransformerClassName);

        if (this.optionsClass == null) {
            if (isBlank(this.optionsClassName)) {
                this.optionsClass = Map.class;
            } else {
                try {
                    this.optionsClass = Class.forName(this.optionsClassName);
                    if (this.optionsClass == null) {
                        throw new Exception("Invalid className");
                    }
                } catch (Exception e) {
                    this.optionsClass = Map.class;
                }
            }
        }
    }
}
