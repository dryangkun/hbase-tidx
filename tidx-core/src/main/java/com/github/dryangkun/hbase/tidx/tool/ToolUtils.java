package com.github.dryangkun.hbase.tidx.tool;

import org.apache.commons.cli.*;
import org.apache.phoenix.util.PhoenixRuntime;

public class ToolUtils {

    public static final String OPTION_HELP_KEY = "help";
    public static final String OPTION_JDBC_URL_KEY = "jdbc-url";
    public static final String OPTION_DATA_TABLE_KEY = "data-table";
    public static final String OPTION_INDEX_NAME_KEY = "index-name";

    public static final Option OPTION_HELP = new Option(null, OPTION_HELP_KEY, false, "help");
    public static final Option OPTION_JDBC_URL = new Option(null, OPTION_JDBC_URL_KEY, true, "phoenix jdbc url");
    public static final Option OPTION_DATA_TABLE = new Option(null, OPTION_DATA_TABLE_KEY, true, "phoenix data table");
    public static final Option OPTION_INDEX_NAME = new Option(null, OPTION_INDEX_NAME_KEY, true, "phoenix local index name");

    public static CommandLine parseCommandLine(Options options, String[] args, Class<?> clazz) {
        CommandLine commandLine = null;
        try {
            commandLine = new GnuParser().parse(options, args);
        } catch (ParseException e) {
            HelpFormatter helpFormatter = new HelpFormatter();
            System.out.println("ERROR:" + e.getMessage());
            helpFormatter.printHelp(clazz.getName(), options);
            System.exit(1);
        }
        if (args.length == 0 || commandLine.hasOption(OPTION_HELP_KEY)) {
            HelpFormatter helpFormatter = new HelpFormatter();
            helpFormatter.printHelp(clazz.getName(), options);
            System.exit(0);
        }
        return commandLine;
    }

    public static String formatPhoenixJdbcUrl(String jdbcUrl) {
        if (jdbcUrl.startsWith(PhoenixRuntime.JDBC_PROTOCOL)) {
            return jdbcUrl;
        } else {
            return PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + jdbcUrl;
        }
    }
}
