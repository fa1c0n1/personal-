package com.apple.aml.stargate.pipeline.pojo;

import org.jgrapht.graph.DefaultEdge;

public class FlowEdge extends DefaultEdge {
    public FlowVertex source() {
        return (FlowVertex) super.getSource();
    }

    public FlowVertex target() {
        return (FlowVertex) super.getTarget();
    }

    @Override
    public String toString() {
        return "FlowEdge{" + super.getSource() + "->" + super.getTarget() + "}";
    }
}
