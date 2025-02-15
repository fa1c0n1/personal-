package com.apple.aml.stargate.cli.boot;

import com.apple.aml.stargate.common.utils.SchemaUtils;
import com.google.common.reflect.ClassPath;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.apple.aml.stargate.common.constants.CommonConstants.SCHEMA_ID;
import static com.apple.aml.stargate.common.utils.AppConfig.appName;
import static com.apple.aml.stargate.common.utils.AppConfig.config;
import static com.apple.aml.stargate.common.utils.AppConfig.mode;
import static com.apple.aml.stargate.common.utils.AvroUtils.avroSchema;
import static com.apple.aml.stargate.common.utils.JsonUtils.jsonString;
import static com.apple.aml.stargate.common.utils.JsonUtils.readJsonMap;
import static com.apple.aml.stargate.common.utils.LogUtils.logger;
import static com.apple.aml.stargate.common.utils.SchemaUtils.doRegisterSchema;

public class RegisterOptionsToSchemaStore {
    public static void main(final String[] args) throws Exception {
        execute();
    }

    @SuppressWarnings("unchecked")
    private static void execute() throws Exception {
        final Logger logger = logger(MethodHandles.lookup().lookupClass());
        final String separator = StringUtils.repeat("=", 60);
        List<String> allPojos = new ArrayList<>(getAllClasses("com.apple.aml.stargate.common.options").stream().map(c -> c.getName()).filter(x -> !x.contains("$")).collect(Collectors.toSet()));
        Collections.sort(allPojos);
        logger.info("List of available pojos", Map.of("classNames", Strings.join(allPojos.iterator(), ',')));
        allPojos.forEach(pojoClass -> {
            logger.info(separator);
            logger.info("Creating AVRO Schema for ", Map.of("className", pojoClass));
            logger.info(separator);
            try {
                Schema baseSchema = avroSchema(Class.forName(pojoClass));
                Map schemaMap = readJsonMap(baseSchema.toString());
                String mode = mode().toLowerCase();
                String namespace = String.format("com.apple.aml.stargate.%s.options", mode);
                schemaMap.put("namespace", namespace);
                String schemaId = namespace + "." + baseSchema.getName();
                Schema.Parser parser = new Schema.Parser();
                Schema schema = parser.parse(jsonString(schemaMap));
                Schema existingSchema = null;
                try {
                    existingSchema = SchemaUtils.schema(schemaId);
                } catch (Exception ignored) {
                }
                if (existingSchema != null && existingSchema.equals(schema)) {
                    return;
                }
                logger.info("Registering schema", Map.of(SCHEMA_ID, schemaId));
                doRegisterSchema(schema.toString(), String.format("%s. Autogenerated schema for config options - %s", appName(), baseSchema.getName()), true, true, null, new HashMap<>(), new HashMap<>(), config().getString("schemas.avro.additional.registration.schemaReference"), logger);
                logger.info("Schema registration successful", Map.of(SCHEMA_ID, schemaId));
            } catch (Exception e) {
                logger.error("Could not register schema for", Map.of("className", pojoClass), e);
            }
            logger.info(separator);
        });

    }

    private static Set<Class> getAllClasses(String packageName) throws IOException {
        return ClassPath.from(ClassLoader.getSystemClassLoader()).getAllClasses().stream().filter(clazz -> clazz.getPackageName().equalsIgnoreCase(packageName)).map(clazz -> clazz.load()).collect(Collectors.toSet());
    }
}
