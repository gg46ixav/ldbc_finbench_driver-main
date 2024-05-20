package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.ldbcouncil.finbench.driver.DbConnectionState;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.NoSuchRecordException;

public class JenaDbConnectionState extends DbConnectionState {

    static class JenaClient {
        private final URL datasetURL;

        JenaClient(String connectionUrl) {
            try {
                datasetURL = new URL(connectionUrl);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        String execute(String body){

            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) datasetURL.openConnection();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            String encoded = Base64.getEncoder().encodeToString(("admin:gbcI5xkMGm7CW48").getBytes(StandardCharsets.UTF_8));
            connection.setRequestProperty("Authorization", "Basic " + encoded);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("host", "localhost:3030");
            connection.setInstanceFollowRedirects(false);
            connection.setUseCaches(false);

            String result = "";
            try {
                connection.setRequestMethod("POST");
            } catch (ProtocolException e) {
                throw new RuntimeException(e);
            }
            connection.setDoOutput(true);
            body = "update=" + body;
            byte[] input = body.getBytes(StandardCharsets.UTF_8);
            connection.setRequestProperty("Content-Length", Integer.toString(input.length));

            try(OutputStream os = connection.getOutputStream()) {

                os.write(input, 0, input.length);
                connection.getInputStream();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return result;
        }
        public void close(){
        }
    }

    private final JenaClient jenaClient;

    public JenaDbConnectionState(String connectionUrl) {
        jenaClient = new JenaClient(connectionUrl);
    }

    JenaClient client() {
        return jenaClient;
    }

    @Override
    public void close() throws IOException {
        jenaClient.close();
    }

}
