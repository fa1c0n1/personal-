package com.apple.aml.stargate.common.pojo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class IASToken {

    private String accessToken;
    private int expiresIn;
    private String idToken;
    private String refreshToken;
    private String scope;
    private String tokenType;

    public IASToken(@JsonProperty("access_token") String accessToken,
                    @JsonProperty("expires_in") int expiresIn,
                    @JsonProperty("id_token") String idToken,
                    @JsonProperty("refresh_token") String refreshToken,
                    @JsonProperty("scope") String scope,
                    @JsonProperty("token_type") String tokenType) {
        this.accessToken = accessToken;
        this.expiresIn = expiresIn;
        this.idToken = idToken;
        this.refreshToken = refreshToken;
        this.scope = scope;
        this.tokenType = tokenType;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder()
                .append(getAccessToken())
                .append(getIdToken())
                .append(getRefreshToken())
                .append(getExpiresIn())
                .append(getScope())
                .append(getTokenType())
                .toHashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        IASToken that = (IASToken) o;

        return new EqualsBuilder()
                .append(getAccessToken(), that.getAccessToken())
                .append(getIdToken(), that.getIdToken())
                .append(getRefreshToken(), that.getRefreshToken())
                .append(getExpiresIn(), that.getExpiresIn())
                .append(getScope(), that.getScope())
                .append(getTokenType(), that.getTokenType())
                .isEquals();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("access_token", getAccessToken())
                .append("id_token", getIdToken())
                .append("refresh_token", getRefreshToken())
                .append("expires_in", getExpiresIn())
                .append("scope", getScope())
                .append("token_type", getTokenType())
                .toString();
    }

}
