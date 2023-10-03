package com.apple.aml.stargate.app.pojo;

import lombok.Data;
import org.bson.codecs.pojo.annotations.BsonProperty;

import java.io.Serializable;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class Schema implements Serializable {

    private static final long serialVersionUID = 1L;

    @BsonProperty(value = "firstEventSeenOn")
    private Date firstEventSeenOn;

    @BsonProperty(value = "schemaId")
    private String schemaId;

    @BsonProperty(value = "lastEventSeenOn")
    private Date lastEventSeenOn;

    @BsonProperty(value = "versionNos")
    private Set<Integer> versionNos;

    @BsonProperty(value = "latestVersion")
    private Integer latestVersion;

    @BsonProperty(value = "stageSnapshots")
    private Set<StageSnapshot> stageSnapshots;

    public boolean updateSchema(Schema schema) {

        if (schemaId != null && !schemaId.equals(schema.schemaId)) {
            return false;
        }

        if (stageSnapshots == null) {
            this.stageSnapshots = schema.stageSnapshots;
        } else if (schema.stageSnapshots != null) {
            Map<String, StageSnapshot> map = convertToMap(stageSnapshots);

            for (StageSnapshot stageSnapshot : schema.stageSnapshots) {
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

    private Map<String, StageSnapshot> convertToMap(Set<StageSnapshot> stageSnapshots) {
        Map<String, StageSnapshot> map = stageSnapshots.stream()
                .collect(Collectors.toMap(StageSnapshot::getName, Function.identity()));
        return map;
    }
}
