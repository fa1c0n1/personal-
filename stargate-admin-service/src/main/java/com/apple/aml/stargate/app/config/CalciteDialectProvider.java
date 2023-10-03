package com.apple.aml.stargate.app.config;

import org.springframework.data.jdbc.repository.config.DialectResolver;
import org.springframework.data.relational.core.dialect.AnsiDialect;
import org.springframework.data.relational.core.dialect.Db2Dialect;
import org.springframework.data.relational.core.dialect.Dialect;
import org.springframework.data.relational.core.dialect.H2Dialect;
import org.springframework.data.relational.core.dialect.HsqlDbDialect;
import org.springframework.data.relational.core.dialect.MySqlDialect;
import org.springframework.data.relational.core.dialect.OracleDialect;
import org.springframework.data.relational.core.dialect.PostgresDialect;
import org.springframework.data.relational.core.dialect.SqlServerDialect;
import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.util.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Optional;

public class CalciteDialectProvider implements DialectResolver.JdbcDialectProvider {
    @Override
    public Optional<Dialect> getDialect(final JdbcOperations operations) {
        return Optional.ofNullable(operations.execute((ConnectionCallback<Dialect>) CalciteDialectProvider::getDialect));
    }

    private static Dialect getDialect(final Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();

        String name = metaData.getDatabaseProductName().toLowerCase(Locale.ENGLISH);

        if (name.contains("hsql")) {
            return HsqlDbDialect.INSTANCE;
        }
        if (name.contains("h2")) {
            return H2Dialect.INSTANCE;
        }
        if (name.contains("mysql") || name.contains("mariadb")) {
            return new MySqlDialect(getIdentifierProcessing(metaData));
        }
        if (name.contains("postgresql")) {
            return PostgresDialect.INSTANCE;
        }
        if (name.contains("microsoft")) {
            return SqlServerDialect.INSTANCE;
        }
        if (name.contains("db2")) {
            return Db2Dialect.INSTANCE;
        }
        if (name.contains("oracle")) {
            return OracleDialect.INSTANCE;
        }
        return AnsiDialect.INSTANCE;
    }

    private static IdentifierProcessing getIdentifierProcessing(DatabaseMetaData metaData) throws SQLException {
        String quoteString = metaData.getIdentifierQuoteString();
        IdentifierProcessing.Quoting quoting = StringUtils.hasText(quoteString) ? new IdentifierProcessing.Quoting(quoteString) : IdentifierProcessing.Quoting.NONE;
        IdentifierProcessing.LetterCasing letterCasing;
        if (metaData.supportsMixedCaseIdentifiers()) {
            letterCasing = IdentifierProcessing.LetterCasing.AS_IS;
        } else if (metaData.storesUpperCaseIdentifiers()) {
            letterCasing = IdentifierProcessing.LetterCasing.UPPER_CASE;
        } else if (metaData.storesLowerCaseIdentifiers()) {
            letterCasing = IdentifierProcessing.LetterCasing.LOWER_CASE;
        } else {
            letterCasing = IdentifierProcessing.LetterCasing.UPPER_CASE;
        }
        return IdentifierProcessing.create(quoting, letterCasing);
    }
}
