package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.util.Map;

import org.ldbcouncil.finbench.driver.DbConnectionState;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.NoSuchRecordException;

public class CypherDbConnectionState extends DbConnectionState {

    static class CypherClient {
        private final Driver driver;
        CypherClient(String connectionUrl) {
            driver = GraphDatabase.driver(connectionUrl);
        }
        String execute(String queryString, Map<String, Object> queryParams) {
            try(Session session = driver.session()){
                Result result = session.run(queryString, queryParams);
                return result.single().toString();
            }catch(NoSuchRecordException e){
                return "";
            }
        }
        public void close(){
            driver.close();
        }
    }

    private final CypherClient cypherClient;

    public CypherDbConnectionState(String connectionUrl) {
        cypherClient = new CypherClient(connectionUrl);
    }

    CypherClient client() {
        return cypherClient;
    }

    @Override
    public void close() throws IOException {
        cypherClient.close();
    }

}
