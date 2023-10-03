package com.apple.aml.stargate.pipeline.pojo;

import org.jgrapht.graph.DefaultEdge;

public class FlinkEdge extends Edge {
    public FlinkVertx source() {
        return (FlinkVertx) super.getSource();
    }

    public FlinkVertx target() {
        return (FlinkVertx) super.getTarget();
    }

    @Override
    public String toString() {
        return "Edge{" + super.getSource() + "->" + super.getTarget() + "}";
    }
}
