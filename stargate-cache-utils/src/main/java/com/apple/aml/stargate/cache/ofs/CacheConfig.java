package com.apple.aml.stargate.cache.ofs;

import com.apple.aml.stargate.cache.utils.IgniteUtils;
import org.apache.ignite.Ignite;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import static com.apple.aml.stargate.common.utils.AppConfig.config;

@Configuration
public class CacheConfig {

    @Bean
    @ConditionalOnExpression("'${stargate.cache.ofs.type}'.startsWith('ignite')")
    public Ignite ignite() throws Exception {
        return IgniteUtils.initIgnite(IgniteUtils.igniteConfiguration());
    }

    @Bean(name = "ignite-jdbc")
    @ConfigurationProperties(prefix = "stargate.cache.ofs.ignite-jdbc.datasource")
    @ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-jdbc")
    public DataSource dataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = "ignite-jdbc-template")
    @ConditionalOnProperty(prefix = "stargate.cache.ofs", name = "type", havingValue = "ignite-jdbc")
    public JdbcTemplate jdbcTemplate(@Qualifier("ignite-jdbc") final DataSource ds) {
        JdbcTemplate template = new JdbcTemplate(ds);
        template.setQueryTimeout(config().getInt("stargate.cache.ofs.ignite-jdbc.datasource.queryTimeout"));
        return template;
    }
}
