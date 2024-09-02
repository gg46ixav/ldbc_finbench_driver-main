package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.NoSuchRecordException;

public class CypherDbConnectionState extends DbConnectionState {

    static class CypherClient {
        private final Driver driver;
        CypherClient(String connectionUrl, String username, String password) {
            driver = GraphDatabase.driver(connectionUrl, AuthTokens.basic(username, password));

        }
        String execute(String queryString, Map<String, Object> queryParams) {
            try(Session session = driver.session()){
                Result result = session.run(queryString, queryParams);

                return resultToString(result);
            }catch(ClientException e){
                return "";
            }
        }

        public String resultToString(Result result) {
            List<Map<String, Object>> recordsList = new ArrayList<>();
            while(result.hasNext()){
                Map<String, Object> recordMap = new HashMap<>();
                org.neo4j.driver.Record record = result.next();
                for(String key: record.keys()){
                    Object o = record.get(key).asObject();
                    if(o instanceof ZonedDateTime) o = Date.from(((ZonedDateTime) o).toInstant());
                    else if(o instanceof LocalDateTime) o = Date.from(((LocalDateTime) o).atZone(ZoneId.of("Etc/UTC")).toInstant());
                    else if(o instanceof List && !((List<?>) o).isEmpty() && ((List<?>) o).get(0) instanceof String) o = ((List<String>) o).stream().map(Long::parseLong).collect(Collectors.toList());

                    recordMap.put(key, o);
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

        public Transaction startTransaction(String query, Map<String, Object> parameters) {
            try (Session session = driver.session()) {
                Transaction tx = session.beginTransaction();
                try {
                    tx.run(query, parameters);
                    return tx;
                } catch (Exception e) {
                    tx.rollback();
                    throw new RuntimeException("Error executing query", e);
                }
            }

        }
        public void close(){
            driver.close();
        }
    }

    private final CypherClient cypherClient;

    public CypherDbConnectionState(Map<String, String> properties) {
        cypherClient = new CypherClient(properties.get("host")+":"+properties.get("port")+"/"+properties.get("path"), properties.get("user"), properties.get("pass"));
    }

    CypherClient client() {
        return cypherClient;
    }

    @Override
    public void close() throws IOException {
        cypherClient.close();
    }

}
