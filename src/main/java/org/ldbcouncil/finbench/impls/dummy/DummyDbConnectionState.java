package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.util.Map;

import org.ldbcouncil.finbench.driver.DbConnectionState;

import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.NoSuchRecordException;

public class DummyDbConnectionState extends DbConnectionState {

    static class BasicClient {
        private final Driver driver;
        BasicClient(String connectionUrl) {
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

    private final BasicClient basicClient;

    public DummyDbConnectionState(String connectionUrl) {
        basicClient = new BasicClient(connectionUrl);
    }

    BasicClient client() {
        return basicClient;
    }

    @Override
    public void close() throws IOException {
        basicClient.close();
    }

}
