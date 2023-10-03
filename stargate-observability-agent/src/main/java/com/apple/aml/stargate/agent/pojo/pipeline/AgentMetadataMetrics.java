package com.apple.aml.stargate.agent.pojo.pipeline;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@Data
public class AgentMetadataMetrics implements Serializable {
    private static final long serialVersionUID = 1L;

    public String id;

    public String type;

    public Set<String> applicableNodes = new HashSet<>();

    public Set<String> applicableSchemaIds = new HashSet<>();

    public Date firstEventSeenOn;

    public Date lastEventSeenOn;

    public boolean updateMetrics(AgentMetadataMetrics metrics) {

        boolean isChanged = false;

        if (this.id.equals(metrics.id)) {
            this.type = metrics.type;

            if (this.applicableNodes != null) {
                int beforeChange = this.applicableNodes.size();
                this.applicableNodes.addAll(metrics.applicableNodes);

                if (beforeChange != this.applicableNodes.size()) {
                    return true;
                }

            } else {
                this.applicableNodes = metrics.applicableNodes;
                isChanged = true;
            }

            if (this.applicableSchemaIds != null) {
                int beforeChange = this.applicableSchemaIds.size();
                this.applicableSchemaIds.addAll(metrics.applicableSchemaIds);

                if (beforeChange != this.applicableSchemaIds.size()) {
                    return true;
                }
            } else {
                this.applicableSchemaIds = metrics.applicableSchemaIds;
                isChanged = true;
            }

            if (this.firstEventSeenOn == null || (metrics.firstEventSeenOn != null && this.firstEventSeenOn.after(metrics.firstEventSeenOn))) {
                this.firstEventSeenOn = metrics.firstEventSeenOn;
            }

            if (this.lastEventSeenOn == null || (metrics.lastEventSeenOn != null && this.lastEventSeenOn.before(metrics.lastEventSeenOn))) {
                this.lastEventSeenOn = metrics.lastEventSeenOn;
            }

            return isChanged;
        }

        return false;
    }
}
