import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: ternovsky
 * Date: 05.10.13
 * Time: 22:17
 * To change this template use File | Settings | File Templates.
 */
public class HTTPClient {

    public static void main(String[] args) throws IOException {

        int count = 10;
        Map<String, String> keyValue = new HashMap<String, String>();
        Random random = new Random();
        for (int i = 0; i < count; i++) {
            int randInt = random.nextInt(10 * count);
            keyValue.put("key" + randInt, "value=" + randInt);
        }

        String[] ports = {"1001", "1002", "1003"};

        for (Map.Entry<String, String> keyValueEntry : keyValue.entrySet()) {

            URL serverAddress = new URL("http://localhost:" + ports[random.nextInt(ports.length)] + "/data/" + keyValueEntry.getKey());
            HttpURLConnection connection = (HttpURLConnection) serverAddress.openConnection();
            connection.setRequestMethod("PUT");
            connection.setDoOutput(true);
            connection.connect();

            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(keyValueEntry.getValue().getBytes());
            outputStream.flush();
            outputStream.close();
            connection.getResponseCode();
            connection.disconnect();
        }

        for (String port : ports) {
            for (Map.Entry<String, String> keyValueEntry : keyValue.entrySet()) {
                String key = keyValueEntry.getKey();
                String value = keyValueEntry.getValue();

                URL serverAddress = new URL("http://localhost:" + port + "/data/" + key);
                HttpURLConnection connection = (HttpURLConnection) serverAddress.openConnection();
                connection.setRequestMethod("GET");
                connection.setDoOutput(true);
                connection.connect();

                InputStream inputStream = connection.getInputStream();
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                StringBuilder builder = new StringBuilder();
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    builder.append(line).append('\n');
                }
                if (!builder.toString().contains(value)) {
                    System.err.println("Key: " + key + ", Value: " + value + ", Gotten value: " + builder.toString());
                }
                inputStream.close();
                connection.disconnect();
            }
        }
    }
}
