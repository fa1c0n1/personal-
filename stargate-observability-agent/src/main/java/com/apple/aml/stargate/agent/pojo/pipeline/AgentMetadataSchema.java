package com.apple.aml.stargate.agent.pojo.pipeline;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class AgentMetadataSchema implements Serializable {

    private static final long serialVersionUID = 1L;

    private Date firstEventSeenOn;

    private String schemaId;

    private Date lastEventSeenOn;

    private Set<Integer> versionNos;

    private Integer latestVersion;

    private Set<AgentMetadataStageSnapshot> stageSnapshots;

    public boolean updateSchema(AgentMetadataSchema schema) {

        if (schemaId != null && !schemaId.equals(schema.schemaId)) {
            return false;
        }

        if (stageSnapshots == null) {
            this.stageSnapshots = schema.stageSnapshots;
        } else if (schema.stageSnapshots != null) {
            Map<String, AgentMetadataStageSnapshot> map = convertToMap(stageSnapshots);

            for (AgentMetadataStageSnapshot stageSnapshot : schema.stageSnapshots) {
                map.put(stageSnapshot.getName(), stageSnapshot);
            }
            stageSnapshots = new HashSet<>(map.values());
        }

        this.latestVersion = schema.latestVersion;

        if (versionNos == null) {
            versionNos = schema.versionNos;
        } else if (schema.versionNos != null) {
            versionNos.addAll(schema.versionNos);
        }

        if (this.firstEventSeenOn == null || (schema.firstEventSeenOn != null && this.firstEventSeenOn.after(schema.firstEventSeenOn))) {
            this.firstEventSeenOn = schema.firstEventSeenOn;
        }

        if (this.lastEventSeenOn == null || (schema.lastEventSeenOn != null && this.lastEventSeenOn.before(schema.lastEventSeenOn))) {
            this.lastEventSeenOn = schema.lastEventSeenOn;
        }

        return true;

    }

    private Map<String, AgentMetadataStageSnapshot> convertToMap(Set<AgentMetadataStageSnapshot> stageSnapshots) {
        Map<String, AgentMetadataStageSnapshot> map = stageSnapshots.stream()
                .collect(Collectors.toMap(AgentMetadataStageSnapshot::getName, Function.identity()));
        return map;
    }
}
