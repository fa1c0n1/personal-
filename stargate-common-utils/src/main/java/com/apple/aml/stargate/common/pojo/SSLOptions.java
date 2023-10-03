package com.apple.aml.stargate.common.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class SSLOptions {
    private String keystorePath;
    private String keystorePassword;
    private String keyPassword;
    private String truststorePath;
    private String truststorePassword;
}
