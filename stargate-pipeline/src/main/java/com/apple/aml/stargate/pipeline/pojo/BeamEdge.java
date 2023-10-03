package com.apple.aml.stargate.pipeline.pojo;


public class BeamEdge extends Edge {
    public BeamVertx source() {
        return (BeamVertx) super.getSource();
    }

    public BeamVertx target() {
        return (BeamVertx) super.getTarget();
    }

    @Override
    public String toString() {
        return "Edge{" + super.getSource() + "->" + super.getTarget() + "}";
    }
}
