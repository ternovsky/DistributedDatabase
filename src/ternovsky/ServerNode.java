package ternovsky;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: ternovsky
 * Date: 05.10.13
 * Time: 21:25
 * To change this template use File | Settings | File Templates.
 */
public class ServerNode {

    private static final String DATA = "data";
    private static final String INTERACTION = "interaction";
    private static final String GET = "GET";
    private static final String PUT = "PUT";
    private static final String SEPARATOR = "/";
    private static Pattern SEPARATOR_PATTER = Pattern.compile(SEPARATOR);

    private Collection<InetSocketAddress> nodeAddresses = new HashSet<InetSocketAddress>();
    private Map<String, Value> data = new ConcurrentHashMap<String, Value>();

    private ServerNode(InetSocketAddress address, String confPath) {

        try {
            //Building node set
            Scanner scanner = new Scanner(new File(confPath));
            while (scanner.hasNextLine()) {
                nodeAddresses.add(Helper.parseHostPort(scanner.nextLine()));
            }
            nodeAddresses.remove(address);

            //Starting http server
            HttpServer server = HttpServer.create(address, 0);
            server.createContext(SEPARATOR, new RequestHandler());
            server.setExecutor(Executors.newCachedThreadPool());
            server.start();
            long time = System.currentTimeMillis();
            System.out.println("Server started at " + new Date(time) + " on " + address);

            for (InetSocketAddress nodeAddress : nodeAddresses) {
                Map<String, Value> records = getRecords(nodeAddress, time, address);
                if (records != null) {
                    data.putAll(records);
                }
            }
        } catch (Exception e) {
            System.err.println("Server didn't start " + e.getMessage());
        }
    }

    private Map<String, Value> getRecords(InetSocketAddress nodeAddress, long time, InetSocketAddress currentAddress) {

        try {
            StringBuilder urlBuilder = new StringBuilder("http://");
            urlBuilder
                    .append(Helper.toString(nodeAddress))
                    .append(SEPARATOR + INTERACTION + SEPARATOR)
                    .append(Helper.toString(currentAddress))
                    .append(SEPARATOR)
                    .append(time);
            URL serverAddress = new URL(urlBuilder.toString());

            HttpURLConnection connection = (HttpURLConnection) serverAddress.openConnection();
            connection.setRequestMethod(GET);
            connection.setDoOutput(true);
            connection.connect();
            InputStream inputStream = connection.getInputStream();
            Map<String, Value> keyValue =
                    (Map<String, Value>) Helper.byteArrayToObject(Helper.getBytesFromInputStream(inputStream));
            data.putAll(keyValue);
            System.err.println(keyValue.size() + " records were gotten from " + nodeAddress);
            inputStream.close();
            connection.disconnect();

            return keyValue;

        } catch (Exception e) {
            System.err.println("Records weren't gotten from " + nodeAddress + ". " + e.getMessage());
        }

        return null;
    }

    private void sendRecordToAnotherNode(String hostPort, String key, Value value) {

        URL serverAddress = null;

        try {
            StringBuilder urlBuilder = new StringBuilder("http://");
            urlBuilder.append(hostPort).append(SEPARATOR + INTERACTION + SEPARATOR).append(key);
            serverAddress = new URL(urlBuilder.toString());

            HttpURLConnection connection = (HttpURLConnection) serverAddress.openConnection();
            connection.setRequestMethod(PUT);
            connection.setDoOutput(true);
            connection.connect();

            byte[] bytes = Helper.objectToByteArray(value);
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(bytes);
            outputStream.flush();
            outputStream.close();

            System.out.println("The record with key '" + key + "' has been sent to " +
                    serverAddress + ". Response code: " + connection.getResponseCode());
            connection.getResponseCode();
            connection.disconnect();
        } catch (Exception e) {
            System.err.println("The record with key '" + key +
                    "' hasn't been sent to " + serverAddress + ". " + e.getMessage());
        }
    }

    private Value getRecordFromAnotherNode(String hostPort, String key) {

        URL serverAddress = null;

        try {
            StringBuilder urlBuilder = new StringBuilder("http://");
            urlBuilder.append(hostPort).append(SEPARATOR + INTERACTION + SEPARATOR).append(key);
            serverAddress = new URL(urlBuilder.toString());

            HttpURLConnection connection = (HttpURLConnection) serverAddress.openConnection();
            connection.setRequestMethod(GET);
            connection.setDoOutput(true);
            connection.connect();
            InputStream inputStream = connection.getInputStream();
            Value value = (Value) Helper.byteArrayToObject(Helper.getBytesFromInputStream(inputStream));
            System.out.println("The record with key '" + key + "' has been gotten from " + serverAddress);
            connection.disconnect();

            return value;
        } catch (Exception e) {
            System.err.println("The record with key '" + key +
                    "' hasn't been gotten from " + serverAddress + ". " + e.getMessage());
        }

        return null;
    }

    public static void main(String[] args) {
        new ServerNode(Helper.parseHostPort(args[0]), args[1]);
    }

    private class RequestHandler implements HttpHandler {

        private static final int SUCCESS = 200;
        private static final int NOT_FOUND = 404;
        private static final int CREATED = 201;
        private static final int OK = 200;

        public void handle(HttpExchange exchange) throws IOException {

            String requestMethod = exchange.getRequestMethod();

            String[] strings = SEPARATOR_PATTER.split(exchange.getRequestURI().getPath());
            String root = strings[1];
            String key = strings[2];

            boolean isInteractionRequest = INTERACTION.equals(root);
            boolean isDataRequest = DATA.equals(root);

            if (strings.length == 3 && (isDataRequest || isInteractionRequest)) {

                if (GET.equals(requestMethod)) {

                    Value value = data.get(key);

                    if (value == null && isDataRequest) {
                        for (InetSocketAddress nodeAddress : nodeAddresses) {
                            value = getRecordFromAnotherNode(Helper.toString(nodeAddress), key);
                            if (value != null) {
                                data.put(key, value);
                                System.out.println("The record with key '" + key + "' has been saved");
                                break;
                            }
                        }
                    }

                    if (value != null) {
                        if (isDataRequest) {
                            byte[] bytes = value.getBytes();
                            OutputStream outputStream = exchange.getResponseBody();
                            exchange.sendResponseHeaders(SUCCESS, bytes.length);
                            outputStream.write(bytes);
                            outputStream.close();
                        } else {
                            byte[] bytes = Helper.objectToByteArray(value);
                            OutputStream outputStream = exchange.getResponseBody();
                            exchange.sendResponseHeaders(SUCCESS, bytes.length);
                            outputStream.write(bytes);
                            outputStream.close();
                        }
                    } else {
                        exchange.sendResponseHeaders(NOT_FOUND, 0);
                        exchange.getResponseBody().close();
                    }

                } else if (PUT.equals(requestMethod)) {

                    Value value = data.get(key);

                    if (isDataRequest) {
                        byte[] bytes = Helper.getBytesFromInputStream(exchange.getRequestBody());
                        value = new Value(bytes, System.currentTimeMillis());
                        data.put(key, value);
                    } else {
                        InputStream inputStream = exchange.getRequestBody();
                        try {
                            byte[] bytes = Helper.getBytesFromInputStream(inputStream);
                            value = (Value) Helper.byteArrayToObject(bytes);
                            data.put(key, value);
                        } catch (ClassNotFoundException e) {
                            inputStream.close();
                        }
                    }
                    System.out.println("The record with key '" + key + "' has been created");

                    if (value == null) {
                        exchange.sendResponseHeaders(CREATED, 0);
                    } else {
                        exchange.sendResponseHeaders(OK, 0);
                    }

                    if (isDataRequest) {
                        for (InetSocketAddress nodeAddress : nodeAddresses) {
                            sendRecordToAnotherNode(Helper.toString(nodeAddress), key, value);
                        }
                    }
                }
            }

            if (strings.length == 4 && isInteractionRequest) {
                long time = Long.parseLong(strings[3]);
                Map<String, Value> responseMap = new HashMap<String, Value>();
                for (Map.Entry<String, Value> keyValueEntry : data.entrySet()) {
                    if (keyValueEntry.getValue().getTime() <= time) {
                        responseMap.put(keyValueEntry.getKey(), keyValueEntry.getValue());
                    }
                }
                byte[] bytes = Helper.objectToByteArray((Serializable) responseMap);
                OutputStream outputStream = exchange.getResponseBody();
                exchange.sendResponseHeaders(SUCCESS, bytes.length);
                outputStream.write(bytes);
                outputStream.close();
            }
        }
    }

    private static class Value implements Serializable {

        private byte[] bytes;
        private long time;

        public Value(byte[] bytes, long time) {
            this.bytes = bytes;
            this.time = time;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public long getTime() {
            return time;
        }

        @Override
        public boolean equals(Object o) {

            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Value value = (Value) o;

            return Arrays.equals(bytes, value.bytes);
        }

        @Override
        public int hashCode() {
            return bytes != null ? Arrays.hashCode(bytes) : 0;
        }
    }
}
