package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

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
                String resultString = result.single().toString();

                System.out.println();
                System.out.println();
                System.out.println(resultString);
                System.out.println();
                System.out.println();

                return resultString;
            }catch(NoSuchRecordException e){
                System.out.println();
                System.out.println();
                System.out.println("No Record");
                System.out.println(e);
                System.out.println();
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
