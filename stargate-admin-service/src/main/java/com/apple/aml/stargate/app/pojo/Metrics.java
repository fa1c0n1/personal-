package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

@Data
public class Metrics implements Serializable {
    private static final long serialVersionUID = 1L;

    @BsonProperty(value = "id")
    public String id;

    @BsonProperty(value = "type")
    public String type;

    @BsonProperty(value = "applicableNodes")
    public Set<String> applicableNodes;

    @BsonProperty(value = "applicableSchemaIds")
    public Set<String> applicableSchemaIds;

    @BsonProperty(value = "firstEventSeenOn")
    public Date firstEventSeenOn;

    @BsonProperty(value = "lastEventSeenOn")
    public Date lastEventSeenOn;

    public boolean updateMetrics(Metrics metrics) {

        if (this.id.equals(metrics.id)) {
            this.type = metrics.type;

            if (this.applicableNodes != null) {
                this.applicableNodes.addAll(metrics.applicableNodes);
            } else {
                this.applicableNodes = metrics.applicableNodes;
            }

            if (this.applicableSchemaIds != null) {
                this.applicableSchemaIds.addAll(metrics.applicableSchemaIds);
            } else {
                this.applicableSchemaIds = metrics.applicableSchemaIds;
            }

            if (this.firstEventSeenOn == null || (metrics.firstEventSeenOn != null && this.firstEventSeenOn.after(metrics.firstEventSeenOn))) {
                this.firstEventSeenOn = metrics.firstEventSeenOn;
            }

            if (this.lastEventSeenOn == null || (metrics.lastEventSeenOn != null && this.lastEventSeenOn.before(metrics.lastEventSeenOn))) {
                this.lastEventSeenOn = metrics.lastEventSeenOn;
            }

            return true;
        }

        return false;
    }
}
