package org.ldbcouncil.finbench.impls.dummy;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.io.IOUtils;
import org.ldbcouncil.finbench.driver.DbConnectionState;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class VirtuosoDbConnectionState extends DbConnectionState {

    static class VirtuosoDbClient {
        private final URL datasetURL;

        VirtuosoDbClient(String connectionUrl) {
            try {
                datasetURL = new URL(connectionUrl);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        String executeWrite(String body){
            return execute(body);
        }
        String execute(String body){

            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) datasetURL.openConnection();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            connection.setRequestProperty("Content-Type", "application/sparql-query");
            connection.setRequestProperty("charset", "utf-8");
            connection.setRequestProperty("Accept", "text/csv");
            connection.setInstanceFollowRedirects(false);
            connection.setUseCaches(false);

            String result = "";
            try {
                connection.setRequestMethod("POST");
            } catch (ProtocolException e) {
                System.out.println(e.getMessage());

                throw new RuntimeException(e);
            }
            connection.setDoOutput(true);
            byte[] input = body.getBytes(StandardCharsets.UTF_8);
            connection.setRequestProperty("Content-Length", Integer.toString(input.length));

            try(OutputStream os = connection.getOutputStream()) {

                os.write(input, 0, input.length);
                InputStream is = connection.getInputStream();
                result = IOUtils.toString(is, StandardCharsets.UTF_8);
            } catch (IOException e) {
                try {
                    System.out.println(IOUtils.toString(connection.getErrorStream(), StandardCharsets.UTF_8));
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
            CsvMapper csvMapper = new CsvMapper();
            CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();
            try {
                MappingIterator<Map<String, String>> iterator = csvMapper.readerFor(Map.class).with(csvSchema).readValues(result);
                List<Map<String, String>> list = iterator.readAll();
                ObjectMapper mapper = new ObjectMapper();
                result = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(list);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return result;
        }
        private String execute(String body, URL datasetURL){

            HttpURLConnection connection = null;
            try {
                connection = (HttpURLConnection) datasetURL.openConnection();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            connection.setRequestProperty("Content-Type", "application/sparql-query");
            connection.setRequestProperty("charset", "utf-8");
            connection.setInstanceFollowRedirects(false);
            connection.setUseCaches(false);

            String result = "";
            try {
                connection.setRequestMethod("POST");
            } catch (ProtocolException e) {
                System.out.println(e.getMessage());

                throw new RuntimeException(e);
            }
            connection.setDoOutput(true);
            byte[] input = body.getBytes(StandardCharsets.UTF_8);
            connection.setRequestProperty("Content-Length", Integer.toString(input.length));

            try(OutputStream os = connection.getOutputStream()) {

                os.write(input, 0, input.length);
                connection.getInputStream();
            } catch (IOException e) {
                System.out.println(e.getMessage());
                throw new RuntimeException(e);
            }
            return result;
        }
        public void close(){
        }
    }

    private final VirtuosoDbClient graphDbClient;

    public VirtuosoDbConnectionState(Map<String, String> properties) {
        graphDbClient = new VirtuosoDbClient(properties.get("host")+":"+properties.get("port")+"/"+properties.get("path"));
    }

    VirtuosoDbClient client() {
        return graphDbClient;
    }

    @Override
    public void close() throws IOException {
        graphDbClient.close();
    }

}
