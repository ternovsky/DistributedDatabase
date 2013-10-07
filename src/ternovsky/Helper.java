package ternovsky;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: ternovsky
 * Date: 06.10.13
 * Time: 12:50
 * To change this template use File | Settings | File Templates.
 */
public class Helper {

    private static final String COLON = ":";
    private static final Pattern COLON_PATTERN = Pattern.compile(COLON);

    public static InetSocketAddress parseHostPort(String hostPort) {

        String[] hostPortPair = COLON_PATTERN.split(hostPort.trim());

        return new InetSocketAddress(hostPortPair[0], Integer.parseInt(hostPortPair[1]));
    }

    public static byte[] getBytesFromInputStream(InputStream inputStream) throws IOException {

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = inputStream.read(buffer, 0, buffer.length)) != -1) {
            byteArrayOutputStream.write(buffer, 0, read);
        }
        byteArrayOutputStream.flush();

        return byteArrayOutputStream.toByteArray();
    }

    public static String toString(InetSocketAddress inetSocketAddress) {

        StringBuilder builder = new StringBuilder();
        builder.append(inetSocketAddress.getHostString()).append(COLON).append(inetSocketAddress.getPort());

        return builder.toString();
    }

    public static byte[] objectToByteArray(Serializable serializable) throws IOException {

        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream);
        objectOutputStream.writeObject(serializable);
        objectOutputStream.flush();
        objectOutputStream.close();

        return byteOutputStream.toByteArray();
    }

    public static Serializable byteArrayToObject(byte[] bytes) throws IOException, ClassNotFoundException {

        ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
        ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
        Serializable serializable = (Serializable) objectInputStream.readObject();
        objectInputStream.close();

        return serializable;
    }
}
