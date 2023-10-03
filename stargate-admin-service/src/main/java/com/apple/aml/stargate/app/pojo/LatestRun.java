package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

@Data
public class LatestRun implements Serializable {
    private static final long serialVersionUID = 1L;

    @BsonProperty(value = "runNo")
    private Long runNo;

    @BsonProperty(value = "startedOn")
    private Date startedOn;

    @BsonProperty(value = "lastEventSeenOn")
    private Date lastEventSeenOn;

    @BsonProperty(value = "noOfWorkers")
    private int noOfWorkers;

    @BsonProperty(value = "healthMetricNames")
    private Set<String> healthMetricNames;

    public boolean updateLatestRun(LatestRun latestRun) {
        if (latestRun == null) {
            return false;
        }

        if (this.startedOn == null || (latestRun.startedOn != null && this.startedOn.after(latestRun.startedOn))) {
            this.startedOn = latestRun.startedOn;
        }

        if (this.lastEventSeenOn == null || (latestRun.lastEventSeenOn != null && this.lastEventSeenOn.before(latestRun.lastEventSeenOn))) {
            this.lastEventSeenOn = latestRun.lastEventSeenOn;
        }

        if (latestRun.healthMetricNames != null) {
            healthMetricNames.addAll(latestRun.healthMetricNames);
        }

        this.noOfWorkers = latestRun.noOfWorkers;
        this.runNo = latestRun.runNo;

        return true;
    }
}
