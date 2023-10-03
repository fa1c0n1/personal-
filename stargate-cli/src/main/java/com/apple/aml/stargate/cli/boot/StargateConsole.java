package com.apple.aml.stargate.cli.boot;

import com.google.common.base.CaseFormat;
import org.slf4j.Logger;

import java.lang.invoke.MethodHandles;

import static com.apple.aml.stargate.common.utils.LogUtils.logger;

public class StargateConsole {
    public static void main(final String[] args) throws Exception {
        final Logger logger = logger(MethodHandles.lookup().lookupClass()); // ensure this is method level variable and not class level to honor --appId setting
        String possibleCommand = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE, args[0].replace('-', '_')));
        COMMAND command;
        try {
            command = COMMAND.valueOf(possibleCommand);
        } catch (Exception ignored) {
            command = null;
        }
        if (command == null) return;
        String[] modified = new String[args.length - 1];
        System.arraycopy(args, 1, modified, 0, args.length - 1);
        logger.info("Processing {} command", command);
        switch (command) {
            case registerOptions:
                RegisterOptionsToSchemaStore.main(modified);
        }
    }

    public enum COMMAND {
        registerOptions
    }
}
