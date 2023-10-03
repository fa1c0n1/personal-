package com.apple.aml.stargate.agent.pojo.pipeline;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.Set;

@Data
public class AgentMetadataLatestRun implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long runNo;

    private Date startedOn;

    private Date lastEventSeenOn;

    private int noOfWorkers;

    private Set<String> healthMetricNames;

    public boolean updateLatestRun(AgentMetadataLatestRun latestRun) {
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
