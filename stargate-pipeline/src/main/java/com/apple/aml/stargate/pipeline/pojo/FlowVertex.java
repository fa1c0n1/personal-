package com.apple.aml.stargate.pipeline.pojo;

import com.apple.aml.stargate.common.pojo.Holder;

public class FlowVertex extends Holder<String> {
    private boolean init;

    public boolean init(final boolean init) {
        this.init = init;
        return this.init;
    }

    public boolean init() {
        return this.init;
    }
}
