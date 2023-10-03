package com.apple.aml.stargate.executor.boot;

import com.apple.aml.stargate.cli.boot.StargateConsole;
import com.apple.aml.stargate.pipeline.boot.Executor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CaseFormat;

import java.io.Closeable;

public final class StargateExecutor extends Executor implements Closeable {
    public static void main(final String[] args) throws Exception {
        overrideAppIdAndApplyDefaults(args);
        if (args.length == 0) {
            Executor.main(args);
            return;
        }
        String possibleCommand = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE, args[0].replace('-', '_')));
        StargateConsole.COMMAND command;
        try {
            command = StargateConsole.COMMAND.valueOf(possibleCommand);
        } catch (Exception ignored) {
            command = null;
        }
        if (command == null) {
            Executor.main(args);
            return;
        }
        StargateConsole.main(args);
    }
}
