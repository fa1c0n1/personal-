package com.apple.aml.stargate.app.pojo;

import com.typesafe.config.Optional;
import lombok.Data;

@Data
public class GithubPathDetails {
    @Optional
    private String org;
    @Optional
    private String repo;
    @Optional
    private String branch;
    @Optional
    private String path;
    @Optional
    private String accessToken;

    public String fullPath() {
        StringBuilder builder = new StringBuilder();
        if (org != null && !org.trim().isBlank()) {
            builder.append("/").append(org.trim());
        }
        if (repo != null && !repo.trim().isBlank()) {
            builder.append("/").append(repo.trim());
        }
        if (branch != null && !branch.trim().isBlank()) {
            builder.append("/").append(branch.trim());
        }
        builder.append("/").append(path.trim());
        return builder.substring(1);
    }
}
