package com.apple.aml.stargate.common.utils;

import javax.net.ServerSocketFactory;
import java.net.InetAddress;
import java.net.ServerSocket;

import static com.apple.aml.stargate.common.utils.AppConfig.env;

public final class NetworkUtils {
    private static final String HOSTNAME = env("HOSTNAME", _hostName()).toLowerCase();

    private NetworkUtils() {
    }

    private static String _hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "localhost";
        }
    }

    public static String hostName() {
        return HOSTNAME;
    }

    public static int nextAvailablePort(final int minPort) {
        return nextAvailablePort(minPort, 65535);
    }

    public static int nextAvailablePort(final int minPort, final int maxPort) {
        for (int i = minPort; i < maxPort; i++) {
            if (isPortAvailable(i)) {
                return i;
            }
        }
        return -1;
    }

    public static boolean isPortAvailable(final int port) {
        try {
            ServerSocket serverSocket = ServerSocketFactory.getDefault().createServerSocket(port, 1);
            serverSocket.close();
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }
}
