package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import org.apache.commons.io.IOUtils;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.Update;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.manager.RemoteRepositoryManager;
import org.ldbcouncil.finbench.driver.DbConnectionState;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.NoSuchRecordException;
import virtuoso.rdf4j.driver.VirtuosoRepository;

public class GraphDbConnectionState extends DbConnectionState {

    static class GraphDbClient {
        private final Repository repository;

        GraphDbClient(String connectionUrl, String repositoryName) {

                 RemoteRepositoryManager repositoryManager = RemoteRepositoryManager.getInstance(connectionUrl);
                 repositoryManager.init();
                 repository = repositoryManager.getRepository(repositoryName);
        }
        GraphDbClient(String connectionUrl, String user, String password) {

            repository = new VirtuosoRepository("jdbc:virtuoso://"+connectionUrl, user, password);
        }
        public RepositoryConnection startTransaction(String body){
            RepositoryConnection connection = repository.getConnection();
            connection.setAutoCommit(false);
            Update update = connection.prepareUpdate(body);
            update.execute();
            return connection;
        }

        public String execute(String body) {
            try (RepositoryConnection connection = repository.getConnection()) {
                TupleQueryResult result = connection.prepareTupleQuery(body).evaluate();
                return resultToString(result);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void executeWrite(String body) {
            try (RepositoryConnection connection = repository.getConnection()) {
                connection.prepareUpdate(body).execute();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void close(){
        }

        public String resultToString(TupleQueryResult result) {
            List<Map<String, Object>> recordsList = new ArrayList<>();
            for(BindingSet solution: result){
                Map<String, Object> recordMap = new HashMap<>();
                for(String key: solution.getBindingNames()){
                    recordMap.put(key, solution.getValue(key).stringValue());
                }
                recordsList.add(recordMap);
            }

            String s = null;
            try {
                s = new ObjectMapper().writeValueAsString(recordsList);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return s;
        }
    }

    private final GraphDbClient graphDbClient;

    public GraphDbConnectionState(Map<String, String> properties) {
        graphDbClient = new GraphDbClient(properties.get("host")+":"+properties.get("port"), properties.get("path"));
    }
    public GraphDbConnectionState(Map<String, String> properties, String virtuoso) {
        graphDbClient = new GraphDbClient(properties.get("host")+":"+properties.get("port"), properties.get("user"), properties.get("pass"));
    }

    GraphDbClient client() {
        return graphDbClient;
    }

    @Override
    public void close() throws IOException {
        graphDbClient.close();
    }

}
