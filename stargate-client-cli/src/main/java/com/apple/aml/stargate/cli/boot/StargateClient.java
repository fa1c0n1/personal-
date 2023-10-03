package com.apple.aml.stargate.cli.boot;

import com.apple.aml.stargate.cli.commands.OperatorSetup;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

@Command(name = "stargate", mixinStandardHelpOptions = true, version = "1.3", description = "command line interface to interact with stargate admin/pipelines", subcommands = {OperatorSetup.class, CommandLine.HelpCommand.class})
public class StargateClient implements Runnable {
    public static void main(String... args) {
        CommandLine cmd = new CommandLine(new StargateClient()).setCaseInsensitiveEnumValuesAllowed(true).setUsageHelpWidth(200).setUsageHelpAutoWidth(true);
        cmd.setExecutionStrategy(new CommandLine.RunAll()); // default is RunLast
        cmd.execute(args);
        if (args.length == 0) {
            cmd.usage(System.out);
        }
    }

    public static boolean isBlank(final String input) {
        if (input == null) return true;
        return input.trim().isBlank();
    }

    public static String toString(final URL url, final Charset encoding) throws IOException, URISyntaxException {
        return Files.readString(Paths.get(url.toURI()), encoding);
    }

    @Override
    public void run() {
    }
}
