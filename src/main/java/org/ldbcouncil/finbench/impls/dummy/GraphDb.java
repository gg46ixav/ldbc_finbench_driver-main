package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;
import org.neo4j.driver.Transaction;

public class GraphDb extends Db {
    static Logger logger = LogManager.getLogger("GraphDb");

    private GraphDbConnectionState connectionState = null;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {

        connectionState = new GraphDbConnectionState(map);
        logger.info("GraphDb initialized");

        // complex reads

        registerOperationHandler(ComplexRead1.class, ComplexRead1Handler.class);
        registerOperationHandler(ComplexRead2.class, ComplexRead2Handler.class);
        registerOperationHandler(ComplexRead3.class, ComplexRead3Handler.class);
        registerOperationHandler(ComplexRead4.class, ComplexRead4Handler.class);
        registerOperationHandler(ComplexRead5.class, ComplexRead5Handler.class);
        registerOperationHandler(ComplexRead6.class, ComplexRead6Handler.class);
        registerOperationHandler(ComplexRead7.class, ComplexRead7Handler.class);

        registerOperationHandler(ComplexRead8.class, ComplexRead8Handler.class);
        registerOperationHandler(ComplexRead9.class, ComplexRead9Handler.class);
        registerOperationHandler(ComplexRead10.class, ComplexRead10Handler.class);
        registerOperationHandler(ComplexRead11.class, ComplexRead11Handler.class);
        registerOperationHandler(ComplexRead12.class, ComplexRead12Handler.class);




        // simple reads
        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
        registerOperationHandler(SimpleRead5.class, SimpleRead5Handler.class);
        registerOperationHandler(SimpleRead6.class, SimpleRead6Handler.class);


        // writes


        registerOperationHandler(Write1.class, Write1Handler.class);

        registerOperationHandler(Write2.class, Write2Handler.class);
        registerOperationHandler(Write3.class, Write3Handler.class);
        registerOperationHandler(Write4.class, Write4Handler.class);
        registerOperationHandler(Write5.class, Write5Handler.class);
        registerOperationHandler(Write6.class, Write6Handler.class);
        registerOperationHandler(Write7.class, Write7Handler.class);
        registerOperationHandler(Write8.class, Write8Handler.class);
        registerOperationHandler(Write9.class, Write9Handler.class);
        registerOperationHandler(Write10.class, Write10Handler.class);
        registerOperationHandler(Write11.class, Write11Handler.class);
        registerOperationHandler(Write12.class, Write12Handler.class);
        registerOperationHandler(Write13.class, Write13Handler.class);
        registerOperationHandler(Write14.class, Write14Handler.class);
        registerOperationHandler(Write15.class, Write15Handler.class);
        registerOperationHandler(Write16.class, Write16Handler.class);

        registerOperationHandler(Write17.class, Write17Handler.class);
        registerOperationHandler(Write18.class, Write18Handler.class);
        registerOperationHandler(Write19.class, Write19Handler.class);



        // read-writes
        registerOperationHandler(ReadWrite1.class, ReadWrite1Handler.class);
        registerOperationHandler(ReadWrite2.class, ReadWrite2Handler.class);
        registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);


    }

    @Override
    protected void onClose() throws IOException {
        logger.info("GraphDb closed");
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr1);

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr1.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr1.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr1.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX account: <http://example.org/Account/> " +
                    "\n" +
                    "SELECT DISTINCT ?otherId ?accountDistance ?mediumId ?mediumType WHERE {\n" +
                    "  # Define the starting account\n" +
                    "  BIND(account:" + cr1.getId() + " AS ?startAccount)\n" +
                    "  \n" +
                    "  # Define the blocked medium\n" +
                    "  ?medium ex:isBlocked true .\n" +
                    "  ?medium ex:mediumType ?mediumType .\n" +
                    "  \n" +
                    "  # Find transfers from the starting account to other accounts with depth 1, 2, and 3\n" +
                    "  {\n" +
                    "    # Depth 1\n" +

                    "    << ?startAccount ex:transfer ?account1 >> ex:occurrences ?occurrence .\n" +
                    "    << ?medium ex:signIn ?account1 >> ex:occurrences ?occurrence1 .\n" +
                    "    ?occurrence1 ex:createTime ?signInTime1 .\n" +
                    "    FILTER(?signInTime1 > xsd:dateTime(\""+ DATE_FORMAT.format(cr1.getStartTime()) +"\") && ?signInTime1 < xsd:dateTime(\""+ DATE_FORMAT.format(cr1.getEndTime()) + "\"))\n" +
                    "    BIND(1 AS ?accountDistance)\n" +
                    "    BIND(?account1 AS ?otherAccount)\n" +
                    "  } UNION {\n" +
                    "    # Depth 2\n" +
                    "    << ?startAccount ex:transfer ?account1 >> ex:occurrences ?occurrence21 .\n" +
                    "    << ?account1 ex:transfer ?account2 >> ex:occurrences ?occurrence22 .\n" +
                    "        ?occurrence21 ex:createTime ?transfer21Time .\n" +
                    "        ?occurrence22 ex:createTime ?transfer22Time .\n" +
                    "        FILTER(?transfer21Time < ?transfer22Time)\n" +
                    "    << ?medium ex:signIn ?account2 >> ex:occurrences ?occurrence2 .\n" +
                    "    ?occurrence2 ex:createTime ?signInTime2 .\n" +
                    "    FILTER(?signInTime2 > xsd:dateTime(\""+DATE_FORMAT.format(cr1.getStartTime())+"\") && ?signInTime2 < xsd:dateTime(\""+DATE_FORMAT.format(cr1.getEndTime())+"\"))\n" +
                    "    BIND(2 AS ?accountDistance)\n" +
                    "    BIND(?account2 AS ?otherAccount)\n" +
                    "  } UNION {\n" +
                    "    # Depth 3\n" +
                    "    << ?startAccount ex:transfer ?account1 >> ex:occurrences ?occurrence31 .\n" +
                    "    << ?account1 ex:transfer ?account2 >> ex:occurrences ?occurrence32 .\n" +
                    "    << ?account2 ex:transfer ?account3 >> ex:occurrences ?occurrence33 .\n" +
                    "        ?occurrence31 ex:createTime ?transfer31Time .\n" +
                    "        ?occurrence32 ex:createTime ?transfer32Time .\n" +
                    "        ?occurrence33 ex:createTime ?transfer33Time .\n" +
                    "        FILTER(?transfer31Time < ?transfer32Time && ?transfer32Time < ?transfer33Time)\n" +
                    "    << ?medium ex:signIn ?account3 >> ex:occurrences ?occurrence3 .\n" +
                    "    ?occurrence3 ex:createTime ?signInTime3 .\n" +
                    "    FILTER(?signInTime3 > xsd:dateTime(\""+DATE_FORMAT.format(cr1.getStartTime())+"\") && ?signInTime3 < xsd:dateTime(\""+DATE_FORMAT.format(cr1.getEndTime())+"\"))\n" +
                    "    BIND(3 AS ?accountDistance)\n" +
                    "    BIND(?account3 AS ?otherAccount)\n" +
                    "  }\n" +
                    "  \n" +
                    "  # Extract IDs\n" +
                    "  BIND(xsd:long(STRAFTER(STR(?otherAccount), \"http://example.org/Account/\")) AS ?otherId)\n" +
                    "  BIND(xsd:long(STRAFTER(STR(?medium), \"http://example.org/Medium/\")) AS ?mediumId)\n" +
                    "}\n" +
                    "ORDER BY ASC(?accountDistance) ASC(?otherId) ASC(?mediumId)";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead1Result> complexRead1Results = null;
            try {
                complexRead1Results = cr1.deserializeResult(result);
                resultReporter.report(complexRead1Results.size(), complexRead1Results, cr1);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr1);
            }


        }
    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr2.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr2.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr2.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX person: <http://example.org/Person/> " +
                    "\n" +
                    "SELECT ?otherId ((round(1000 * SUM(distinct ?loanAmount))/1000) AS ?sumLoanAmount) ((round(1000 * SUM(distinct ?loanBalance))/1000) AS ?sumLoanBalance) WHERE {\n" +
                    "  # Define the person and their owned accounts\n" +
                    "    << person:"+ cr2.getId() + " ex:own ?startAccount >> ex:occurrences ?occurrence .\n" +
                    "  # Define the paths for transfers with a depth of 1 to 3\n" +
                    "  {\n" +
                    "    # Depth 1\n" +
                    "    << ?otherAccount1 ex:transfer ?startAccount >> ex:occurrences ?occurrence1 .\n" +
                    "        ?occurrence1 ex:createTime ?transferTime1 .\n" +
                    "    FILTER(?transferTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    BIND(?otherAccount1 AS ?otherAccount)\n" +
                    "    BIND(1 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 2\n" +
                    "    << ?otherAccount2 ex:transfer ?otherAccount1 >> ex:occurrences ?occurrence21 .\n" +
                    "    << ?otherAccount1 ex:transfer ?startAccount >> ex:occurrences ?occurrence22 .\n" +
                    "        ?occurrence21 ex:createTime ?transferTime21 .\n" +
                    "        ?occurrence22 ex:createTime ?transferTime22 .\n" +
                    "    FILTER(?transferTime21 > ?transferTime22)\n" +
                    "    FILTER(?transferTime21 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime21 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime22 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime22 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    BIND(?otherAccount2 AS ?otherAccount)\n" +
                    "    BIND(2 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 3\n" +
                    "    << ?otherAccount3 ex:transfer ?otherAccount2 >> ex:occurrences ?occurrence31 .\n" +
                    "    << ?otherAccount2 ex:transfer ?otherAccount1 >> ex:occurrences ?occurrence32 .\n" +
                    "    << ?otherAccount1 ex:transfer ?startAccount >> ex:occurrences ?occurrence33 .\n" +
                    "        ?occurrence31 ex:createTime ?transferTime31 .\n" +
                    "        ?occurrence32 ex:createTime ?transferTime32 .\n" +
                    "        ?occurrence33 ex:createTime ?transferTime33 .\n" +
                    "    FILTER(?transferTime31 > ?transferTime32 && ?transferTime32 > ?transferTime33)\n" +
                    "    FILTER(?transferTime31 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime31 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime32 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime32 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime33 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime33 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    BIND(?otherAccount3 AS ?otherAccount)\n" +
                    "    BIND(3 AS ?distance)\n" +
                    "  }\n" +
                    "  \n" +
                    "  # Define the loans deposited in the other accounts\n" +
                    "  << ?loan ex:deposit ?otherAccount >> ex:occurrences ?occurrenceL .\n" +
                    "    ?occurrenceL ex:createTime ?loanTime .\n" +
                    "  FILTER(?loanTime > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?loanTime < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "  \n" +
                    "  # Extract loan amount and balance\n" +
                    "  ?loan ex:loanAmount ?loanAmount .\n" +
                    "  ?loan ex:balance ?loanBalance .\n" +
                    "  \n" +
                    "  # Extract other account ID\n" +
                    "  BIND(xsd:long(STRAFTER(STR(?otherAccount), \"http://example.org/Account/\")) AS ?otherId)\n" +
                    "}\n" +
                    "GROUP BY ?otherId\n" +
                    "ORDER BY DESC(?sumLoanAmount) ASC(?otherId)";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead2Result> complexRead2Results = null;
            try {
                complexRead2Results = cr2.deserializeResult(result);
                resultReporter.report(complexRead2Results.size(), complexRead2Results, cr2);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr2);
                resultReporter.report(0, new ArrayList<>(), cr2);
            }
        }
    }

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr3.getId1());
            queryParams.put("id2", cr3.getId2());
            queryParams.put("start_time", DATE_FORMAT.format(cr3.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr3.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX path: <http://www.ontotext.com/path#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX account: <http://example.org/Account/> " +
                    "\n" +
                    "SELECT ?shortestPathLength WHERE {\n" +
                    "  # Define the starting and ending accounts\n" +
                    "  VALUES (?src ?dst) {\n" +
                    "    (account:"+cr3.getId1() +" account:" +cr3.getId2()+ ")\n" +
                    "  }\n" +
                    "\n" +
                    "    OPTIONAL{\n" +
                    "  SERVICE path:search {\n" +
                    "        <urn:path> path:findPath path:distance ;\n" +
                    "                   path:sourceNode ?src ;\n" +
                    "                   path:destinationNode ?dst ;\n" +
                    "                   path:startNode ?start;\n" +
                    "                   path:endNode ?end;\n" +
                    "                   path:distanceBinding ?dist ;\n" +
                    "        \t\t   path:maxPathLength 100 .\n" +
                    "        SERVICE <urn:path> {\n" +
                    "            <<?start ex:transfer ?end>> ex:occurrences ?occurrence .\n" +
                    "            ?occurrence ex:createTime ?createTime\n" +
                    "            FILTER(?createTime>xsd:dateTime(\" "+ DATE_FORMAT.format(cr3.getStartTime()) +"\") && ?createTime<xsd:dateTime(\" "+ DATE_FORMAT.format(cr3.getEndTime()) + "\"))\n" +
                    "        }\n" +
                    "    }\n" +
                    "  }\n" +
                    "\n" +
                    "    BIND(COALESCE(?dist, -1) AS ?shortestPathLength)\n" +
                    "}\n";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead3Result> complexRead3Results = null;
            try {
                complexRead3Results = cr3.deserializeResult(result);
                resultReporter.report(complexRead3Results.size(), complexRead3Results, cr3);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr3);
                resultReporter.report(0, new ArrayList<>(), cr3);
            }
        }
    }

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr4.getId1());
            queryParams.put("id2", cr4.getId2());
            queryParams.put("start_time", DATE_FORMAT.format(cr4.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr4.getEndTime()));

            String queryString = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX account: <http://example.org/Account/> " +
                    "\n" +
                    "SELECT \n" +
                    "?otherId " +
                    "   (round(1000*MAX(?amount2))/1000 AS ?maxEdge2Amount)" +
                    "   (round(1000*SUM(DISTINCT ?amount2))/1000 AS ?sumEdge2Amount)" +
                    "   (COUNT(DISTINCT ?occurrence2) AS ?numEdge2)" +
                    "   (round(1000*MAX(?amount3))/1000 AS ?maxEdge3Amount) " +
                    "   (round(1000*SUM(DISTINCT ?amount3))/1000 AS ?sumEdge3Amount) " +
                    "   (COUNT(DISTINCT ?occurrence3) AS ?numEdge3)" +
                    "WHERE {\n" +
                    "    VALUES (?src ?dst) {\n" +
                    "        (account:"+cr4.getId1()+" account:"+cr4.getId2()+")\n" +
                    "    }\n" +
                    "    \n" +
                    "    # Step 1: Check if src transferred money to dst within the time window\n" +
                    "    <<?src ex:transfer ?dst>> ex:occurrences ?occurrence1 .\n" +
                    "    ?occurrence1 ex:createTime ?createTime1 .\n" +
                    "    ?occurrence1 ex:amount ?amount1 .\n" +
                    "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr4.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr4.getEndTime())+"\"))\n" +
                    "    \n" +
                    "    # Step 2: Find all other accounts that received money from dst and transferred money to src\n" +
                    "    <<?dst ex:transfer ?other>> ex:occurrences ?occurrence3 .\n" +
                    "    ?occurrence3 ex:createTime ?createTime3 .\n" +
                    "    ?occurrence3 ex:amount ?amount3 .\n" +
                    "    FILTER(?createTime3 > xsd:dateTime(\""+DATE_FORMAT.format(cr4.getStartTime())+"\") && ?createTime3 < xsd:dateTime(\""+DATE_FORMAT.format(cr4.getEndTime())+"\"))\n" +
                    "    \n" +
                    "    <<?other ex:transfer ?src>> ex:occurrences ?occurrence2 .\n" +
                    "    ?occurrence2 ex:createTime ?createTime2 .\n" +
                    "    ?occurrence2 ex:amount ?amount2 .\n" +
                    "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(cr4.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(cr4.getEndTime())+"\"))\n" +
                    "    \n" +
                    "    BIND(xsd:long(STRAFTER(STR(?other), \"http://example.org/Account/\")) AS ?otherId)\n" +
                    "}\n" +
                    "GROUP BY ?otherId\n";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead4Result> complexRead4Results = null;
            try {
                complexRead4Results = cr4.deserializeResult(result);
                resultReporter.report(complexRead4Results.size(), complexRead4Results, cr4);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr4);
                resultReporter.report(0, new ArrayList<>(), cr4);
            }
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr5.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr5.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr5.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX person: <http://example.org/Person/> " +
                    "\n" +
                    "SELECT DISTINCT ?startAccountId ?otherAccount1Id ?otherAccount2Id ?otherAccount3Id WHERE {\n" +
                    "  # Define the person and their owned accounts\n" +
                    "    << person:"+cr5.getId()+" ex:own ?startAccount >> ex:occurrences ?occurrence .\n" +
                    "  \n" +
                    "  # Define the paths for transfers with a depth of 1 to 3\n" +
                    "  {\n" +
                    "    # Depth 1\n" +
                    "    << ?startAccount ex:transfer ?otherAccount1 >> ex:occurrences ?occurrence1 .\n" +
                    "        ?occurrence1 ex:createTime ?transferTime1 .\n" +
                    "    FILTER(?transferTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    BIND(STRAFTER(STR(?otherAccount1), \"http://example.org/Account/\") AS ?otherAccount1Id)\n" +
                    "    BIND(STRAFTER(STR(?startAccount), \"http://example.org/Account/\") AS ?startAccountId)\n" +
                    "    BIND(1 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 2\n" +
                    "    << ?startAccount ex:transfer ?otherAccount21 >> ex:occurrences ?occurrence21 .\n" +
                    "    << ?otherAccount21 ex:transfer ?otherAccount22 >> ex:occurrences ?occurrence22 .\n" +
                    "        ?occurrence21 ex:createTime ?transferTime21 .\n" +
                    "        ?occurrence22 ex:createTime ?transferTime22 .\n" +
                    "    FILTER(?transferTime21 < ?transferTime22)\n" +
                    "    FILTER(?transferTime21 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime21 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime22 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime22 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    BIND(STRAFTER(STR(?otherAccount21), \"http://example.org/Account/\") AS ?otherAccount1Id)\n" +
                    "    BIND(STRAFTER(STR(?otherAccount22), \"http://example.org/Account/\") AS ?otherAccount2Id)\n" +
                    "    BIND(STRAFTER(STR(?startAccount), \"http://example.org/Account/\") AS ?startAccountId)\n" +
                    "    BIND(2 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 3\n" +
                    "    << ?startAccount ex:transfer ?otherAccount31 >> ex:occurrences ?occurrence31 .\n" +
                    "    << ?otherAccount31 ex:transfer ?otherAccount32 >> ex:occurrences ?occurrence32 .\n" +
                    "    << ?otherAccount32 ex:transfer ?otherAccount33 >> ex:occurrences ?occurrence33 .\n" +
                    "        ?occurrence31 ex:createTime ?transferTime31 .\n" +
                    "        ?occurrence32 ex:createTime ?transferTime32 .\n" +
                    "        ?occurrence33 ex:createTime ?transferTime33 .\n" +
                    "    FILTER(?transferTime31 < ?transferTime32 && ?transferTime32 < ?transferTime33)\n" +
                    "    FILTER(?transferTime31 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime31 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime32 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime32 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime33 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime33 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    BIND(STRAFTER(STR(?otherAccount31), \"http://example.org/Account/\") AS ?otherAccount1Id)\n" +
                    "    BIND(STRAFTER(STR(?otherAccount32), \"http://example.org/Account/\") AS ?otherAccount2Id)\n" +
                    "    BIND(STRAFTER(STR(?otherAccount33), \"http://example.org/Account/\") AS ?otherAccount3Id)\n" +
                    "    BIND(STRAFTER(STR(?startAccount), \"http://example.org/Account/\") AS ?startAccountId)\n" +
                    "    BIND(3 AS ?distance)\n" +
                    "  }\n" +
                    "  \n" +
                    "}\n";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead5RDFResult> complexRead5RDFResults = null;
            try {
                complexRead5RDFResults = Arrays.asList(new ObjectMapper().readValue(result, ComplexRead5RDFResult[].class));

                List<ComplexRead5Result> complexRead5Results = new ArrayList<>();
                for(ComplexRead5RDFResult c: complexRead5RDFResults){
                    complexRead5Results.add(new ComplexRead5Result(c.getPath()));
                }
                resultReporter.report(complexRead5Results.size(), complexRead5Results, cr5);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr5);
                resultReporter.report(0, new ArrayList<>(), cr5);
            }
        }
    }

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr6.getId());
            queryParams.put("threshold1", cr6.getThreshold1());
            queryParams.put("threshold2", cr6.getThreshold2());
            queryParams.put("start_time", DATE_FORMAT.format(cr6.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr6.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX account: <http://example.org/Account/> " +
                    "\n" +
                    "SELECT ?midId ((round(1000 * SUM(distinct ?edge1Amount))/1000) AS ?sumEdge1Amount) ((round(1000 * SUM(distinct ?edge2Amount))/1000) AS ?sumEdge2Amount) WHERE {\n" +
                    "\n" +
                    "  # Filter for the first edge (transfer)\n" +
                    "  <<?src1 ex:transfer ?mid>> ex:occurrences ?edge1Occurrence .\n" +
                    "  ?edge1Occurrence ex:createTime ?edge1CreateTime ;\n" +
                    "                   ex:amount ?edge1Amount .\n" +
                    "  FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr6.getStartTime())+"\") < ?edge1CreateTime && ?edge1CreateTime < xsd:dateTime(\""+DATE_FORMAT.format(cr6.getEndTime())+"\"))\n" +
                    "  FILTER(?edge1Amount > "+cr6.getThreshold1()+")\n" +
                    "\n" +
                    "  # Filter for the second edge (withdraw)\n" +
                    "  <<?mid ex:withdraw account:"+cr6.getId()+">> ex:occurrences ?edge2Occurrence .\n" +
                    "  ?edge2Occurrence ex:createTime ?edge2CreateTime ;\n" +
                    "                   ex:amount ?edge2Amount .\n" +
                    "  FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr6.getStartTime())+"\") < ?edge2CreateTime && ?edge2CreateTime < xsd:dateTime(\""+DATE_FORMAT.format(cr6.getEndTime())+"\"))\n" +
                    "  FILTER(?edge2Amount > "+cr6.getThreshold2()+")\n" +
                    "  \n" +
                    "  BIND(xsd:long(STRAFTER(STR(?mid), \"http://example.org/Account/\")) AS ?midId)\n" +
                    "} GROUP BY ?midId HAVING (COUNT(?src1) > 3)\n" +
                    "ORDER BY DESC(?sumEdge2Amount) ASC(?midId) \n";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead6Result> complexRead6Results = null;
            try {
                complexRead6Results = cr6.deserializeResult(result);
                resultReporter.report(complexRead6Results.size(), complexRead6Results, cr6);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr6);
                resultReporter.report(0, new ArrayList<>(), cr6);
            }
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr7.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr7.getId());
            queryParams.put("threshold", cr7.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr7.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr7.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX account: <http://example.org/Account/> " +
                    "\n" +
                    "SELECT (COUNT(DISTINCT ?src) AS ?numSrc) \n" +
                    "       (COUNT(DISTINCT ?dst) AS ?numDst) \n" +
                    "       (IF(SUM(?edge2Amount) > 0, ROUND(1000 * (SUM(DISTINCT ?edge1Amount) / SUM(DISTINCT ?edge2Amount))) / 1000, 0) AS ?inOutRatio) \n" +
                    "WHERE {\n" +
                    "    \n" +
                    "  BIND(account:"+cr7.getId()+" AS ?mid)\n" +
                    "  # Match the source, intermediary, and destination accounts\n" +
                    "  \n" +
                    "  # Match the transfer edges and retrieve their amounts\n" +
                    "  <<?src ex:transfer ?mid>> ex:occurrences ?edge1Occurrence .\n" +
                    "  ?edge1Occurrence ex:amount ?edge1Amount .\n" +
                    "  ?edge1Occurrence ex:createTime ?createTime1 .\n" +
                    "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr7.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr7.getEndTime())+"\"))\n" +
                    "    FILTER(?edge1Amount > "+cr7.getThreshold()+")\n" +
                    "    \n" +
                    "  <<?mid ex:transfer ?dst>> ex:occurrences ?edge2Occurrence .\n" +
                    "  ?edge2Occurrence ex:amount ?edge2Amount .\n" +
                    "  ?edge2Occurrence ex:createTime ?createTime2 .\n" +
                    "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(cr7.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(cr7.getEndTime())+"\"))\n" +
                    "    FILTER(?edge2Amount > "+cr7.getThreshold()+")\n" +
                    "}\n" +
                    "\n" +
                    "ORDER BY DESC(?inOutRatio)";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead7Result> complexRead7Results = null;
            try {
                complexRead7Results = cr7.deserializeResult(result);
                resultReporter.report(complexRead7Results.size(), complexRead7Results, cr7);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr7);
                resultReporter.report(0, new ArrayList<>(), cr7);
            }
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr8.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr8.getId());
            queryParams.put("threshold", cr8.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr8.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr8.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "\n" +
                    "SELECT ?dstId (ROUND(1000 * (sum(DISTINCT ?lastAmount) / max(?loanAmount))) / 1000 AS ?ratio) (max(?minDistanceFromLoanA) AS ?minDistanceFromLoan) WHERE {\n" +
                    "  # Define the person and their owned accounts\n" +
                    " loan:"+cr8.getId()+" ex:loanAmount ?loanAmount ." +
                    "  << loan:"+cr8.getId()+" ex:deposit ?startAccount >> ex:occurrences ?occurrence .\n" +
                    "       ?occurrence ex:createTime ?depositTime ." +
                    "  FILTER(?depositTime > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?depositTime < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "  \n" +
                    "  # Define the paths for transfers or withdraw with a depth of 1 to 3\n" +
                    "  {\n" +
                    "    # Depth 1\n" +
                    "    << ?startAccount ?predicate ?otherAccount1 >> ex:occurrences ?occurrence1 .\n" +
                    "        ?occurrence1 ex:createTime ?transferTime1 .\n" +
                    "        ?occurrence1 ex:amount ?edgeAmount .\n" +
                    "    FILTER(?predicate = ex:transfer || ?predicate = ex:withdraw) " +
                    "    FILTER(?transferTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?transferTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "    FILTER(?edgeAmount > "+cr8.getThreshold()+")\n" +
                    "    BIND(?otherAccount1 AS ?otherAccount)\n" +
                    "    BIND(2 AS ?minDistanceFromLoanA)\n" +
                    "    BIND(?edgeAmount AS ?lastAmount) " + // # Total amount for depth 1"
                    "  } UNION {\n" +
                    "    # Depth 2\n" +
                    "    << ?startAccount ?predicate21 ?otherAccount21 >> ex:occurrences ?occurrence21 .\n" +
                    "    FILTER(?predicate21 = ex:transfer || ?predicate21 = ex:withdraw) " +
                    "    << ?otherAccount21 ?predicate22 ?otherAccount22 >> ex:occurrences ?occurrence22 .\n" +
                    "    FILTER(?predicate22 = ex:transfer || ?predicate22 = ex:withdraw) " +
                    "        ?occurrence21 ex:createTime ?transferTime21 .\n" +
                    "        ?occurrence22 ex:createTime ?transferTime22 .\n" +
                    "        ?occurrence21 ex:amount ?edgeAmount21 .\n" +
                    "        ?occurrence22 ex:amount ?edgeAmount22 .\n" +
                    //"    FILTER(?transferTime21 < ?transferTime22)\n" +
                    "    FILTER(?transferTime21 > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?transferTime21 < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime22 > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?transferTime22 < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "    FILTER(?edgeAmount21 > "+cr8.getThreshold()+")\n" +
                    "    FILTER(?edgeAmount22 > "+cr8.getThreshold()+")\n" +
                    "    BIND(?otherAccount22 AS ?otherAccount)\n" +
                    "    BIND(3 AS ?minDistanceFromLoanA)\n" +
                    "    BIND(?edgeAmount22 AS ?lastAmount)" + // # Total amount for depth 2
                    "  } UNION {\n" +
                    "    # Depth 3\n" +
                    "    << ?startAccount ?predicate31 ?otherAccount31 >> ex:occurrences ?occurrence31 .\n" +
                    "    FILTER(?predicate31 = ex:transfer || ?predicate31 = ex:withdraw) " +
                    "    << ?otherAccount31 ?predicate32 ?otherAccount32 >> ex:occurrences ?occurrence32 .\n" +
                    "    FILTER(?predicate32 = ex:transfer || ?predicate32 = ex:withdraw) " +
                    "    << ?otherAccount32 ?predicate33 ?otherAccount33 >> ex:occurrences ?occurrence33 .\n" +
                    "    FILTER(?predicate33 = ex:transfer || ?predicate33 = ex:withdraw) " +
                    "        ?occurrence31 ex:createTime ?transferTime31 .\n" +
                    "        ?occurrence32 ex:createTime ?transferTime32 .\n" +
                    "        ?occurrence33 ex:createTime ?transferTime33 .\n" +
                    "        ?occurrence31 ex:amount ?edgeAmount31 .\n" +
                    "        ?occurrence32 ex:amount ?edgeAmount32 .\n" +
                    "        ?occurrence33 ex:amount ?edgeAmount33 .\n" +
                    //"    FILTER(?transferTime31 < ?transferTime32 && ?transferTime32 < ?transferTime33)\n" +
                    "    FILTER(?transferTime31 > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?transferTime31 < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime32 > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?transferTime32 < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime33 > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?transferTime33 < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "    FILTER(?edgeAmount31 > "+cr8.getThreshold()+")\n" +
                    "    FILTER(?edgeAmount32 > "+cr8.getThreshold()+")\n" +
                    "    FILTER(?edgeAmount33 > "+cr8.getThreshold()+")\n" +
                    "    BIND(?otherAccount33 AS ?otherAccount)\n" +
                    "    BIND(4 AS ?minDistanceFromLoanA)\n" +
                    "    BIND(?edgeAmount33 AS ?lastAmount) " + //# Total amount for depth 3
                    "  }\n" +
                    "  BIND(xsd:long(STRAFTER(STR(?otherAccount), \"http://example.org/Account/\")) AS ?dstId)\n" +
                    "  \n" +
                    "} " +
                    "\n" +
                    "GROUP BY ?dstId " +
                    "ORDER BY DESC(?minDistanceFromLoan) DESC(?ratio) ASC(?dstId)";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead8Result> complexRead8Results = null;
            try {
                complexRead8Results = cr8.deserializeResult(result);
                resultReporter.report(complexRead8Results.size(), complexRead8Results, cr8);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr8);
                resultReporter.report(0, new ArrayList<>(), cr8);
            }
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr9.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr9.getId());
            queryParams.put("threshold", cr9.getThreshold());
            queryParams.put("lowerbound", 0);
            queryParams.put("upperbound", 2147483647);
            queryParams.put("start_time", DATE_FORMAT.format(cr9.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr9.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX account: <http://example.org/Account/>\n" +
                    "\n" +
                    "SELECT \n" +
                    "  (IF(?sumEdge2Amount > 0, ROUND(1000 * (?sumEdge1Amount1 / (?sumEdge2Amount / 1.0))) / 1000, -1) AS ?ratioRepay)\n" +
                    "  (IF(?sumEdge4Amount > 0, ROUND(1000 * (?sumEdge1Amount1 / (?sumEdge4Amount / 1.0))) / 1000, -1) AS ?ratioDeposit)\n" +
                    "  (IF(?sumEdge4Amount > 0, ROUND(1000 * (?sumEdge3Amount1 / (?sumEdge4Amount / 1.0))) / 1000, -1) AS ?ratioTransfer)\n" +
                    "WHERE {\n" +
                    "  \n" +
                    "  # Subquery for edge1Amount\n" +
                    "  {\n" +
                    "    SELECT (SUM(?edge1Amount) AS ?sumEdge1Amount1)\n" +
                    "    WHERE {\n" +
                    "      BIND(account:" + cr9.getId() + " AS ?startAccount)\n" +
                    "      OPTIONAL {\n" +
                    "        << ?loan ex:deposit ?startAccount >> ex:occurrences ?edge1 .\n" +
                    "        ?edge1 ex:amount ?edge1Amount ;\n" +
                    "               ex:createTime ?edge1CreateTime .\n" +
                    "        FILTER(?edge1Amount > xsd:double(" + cr9.getThreshold() + "))\n" +
                    "        FILTER(xsd:datetime(\"" + DATE_FORMAT.format(cr9.getStartTime()) + "\") < ?edge1CreateTime && ?edge1CreateTime < xsd:datetime(\"" + DATE_FORMAT.format(cr9.getEndTime()) + "\"))\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "  \n" +
                    "  # Subquery for edge2Amount\n" +
                    "  {\n" +
                    "    SELECT (SUM(?edge2Amount) AS ?sumEdge2Amount)\n" +
                    "    WHERE {\n" +
                    "      BIND(account:" + cr9.getId() + " AS ?startAccount)\n" +
                    "      OPTIONAL {\n" +
                    "        # Reification for << ?startAccount ex:repay ?loan >>\n" +
                    "        << ?startAccount ex:repay ?loan >> ex:occurrences ?edge2 .\n" +
                    "        ?edge2 ex:amount ?edge2Amount ;\n" +
                    "               ex:createTime ?edge2CreateTime .\n" +
                    "        FILTER(?edge2Amount > xsd:double(" + cr9.getThreshold() + "))\n" +
                    "        FILTER(xsd:datetime(\"" + DATE_FORMAT.format(cr9.getStartTime()) + "\") < ?edge2CreateTime && ?edge2CreateTime < xsd:datetime(\"" + DATE_FORMAT.format(cr9.getEndTime()) + "\"))\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "  \n" +
                    "  # Subquery for edge3Amount\n" +
                    "  {\n" +
                    "    SELECT (SUM(?edge3Amount) AS ?sumEdge3Amount1)\n" +
                    "    WHERE {\n" +
                    "      BIND(account:" + cr9.getId() + " AS ?startAccount)\n" +
                    "      OPTIONAL {\n" +
                    "        << ?upAccount ex:transfer ?startAccount >> ex:occurrences ?edge3 .\n" +
                    "        ?edge3 ex:amount ?edge3Amount ;\n" +
                    "               ex:createTime ?edge3CreateTime .\n" +
                    "        FILTER(?edge3Amount > xsd:double(" + cr9.getThreshold() + "))\n" +
                    "        FILTER(xsd:datetime(\"" + DATE_FORMAT.format(cr9.getStartTime()) + "\") < ?edge3CreateTime && ?edge3CreateTime < xsd:datetime(\"" + DATE_FORMAT.format(cr9.getEndTime()) + "\"))\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "  \n" +
                    "  # Subquery for edge4Amount\n" +
                    "  {\n" +
                    "    SELECT (SUM(?edge4Amount) AS ?sumEdge4Amount)\n" +
                    "    WHERE {\n" +
                    "      BIND(account:" + cr9.getId() + " AS ?startAccount)\n" +
                    "      OPTIONAL {\n" +
                    "        << ?startAccount ex:transfer ?downAccount >> ex:occurrences ?edge4 .\n" +
                    "        ?edge4 ex:amount ?edge4Amount ;\n" +
                    "               ex:createTime ?edge4CreateTime .\n" +
                    "        FILTER(?edge4Amount > xsd:double(" + cr9.getThreshold() + "))\n" +
                    "        FILTER(xsd:datetime(\"" + DATE_FORMAT.format(cr9.getStartTime()) + "\") < ?edge4CreateTime && ?edge4CreateTime < xsd:datetime(\"" + DATE_FORMAT.format(cr9.getEndTime()) + "\"))\n" +
                    "      }\n" +
                    "    }\n" +
                    "  }\n" +
                    "\n" +
                    "  BIND(COALESCE(?sumEdge1Amount1, 0) AS ?sumEdge1Amount1)\n" +
                    "  BIND(COALESCE(?sumEdge3Amount1, 0) AS ?sumEdge3Amount1)\n" +
                    "}\n" +
                    "\n";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead9Result> complexRead9Results = null;
            try {
                complexRead9Results = cr9.deserializeResult(result);
                resultReporter.report(complexRead9Results.size(), complexRead9Results, cr9);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr9);
                resultReporter.report(0, new ArrayList<>(), cr9);
            }
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr10.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr10.getPid1());
            queryParams.put("id2", cr10.getPid2());
            queryParams.put("start_time", DATE_FORMAT.format(cr10.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr10.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX person: <http://example.org/Person/> " +
                    "\n" +
                    "SELECT (IF(BOUND(?unionSize) && ?unionSize > 0, ROUND(1000 * (?intersectionSize / ?unionSize))/1000, 0) AS ?jaccardSimilarity) WHERE {  \n" +
                    "    \n" +
                    "    # Intersection Size\n" +
                    "    {\n" +
                    "        SELECT (COUNT(DISTINCT ?company) AS ?intersectionSize) WHERE {\n" +
                    "            VALUES (?person1 ?person2) {\n" +
                    "        (person:"+ cr10.getPid1() +" person:"+ cr10.getPid2() +")\n" +
                    "    }\n" +
                    "            << ?person1 ex:invest ?company >> ex:occurrences ?invest1 .\n" +
                    "            ?invest1 ex:createTime ?invest1Time .\n" +
                    "                   FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr10.getStartTime())+"\")<?invest1Time && ?invest1Time < xsd:dateTime(\""+DATE_FORMAT.format(cr10.getEndTime())+"\")) " +
                    "            \n" +
                    "            << ?person2 ex:invest ?company >> ex:occurrences ?invest2 .\n" +
                    "            ?invest2 ex:createTime ?invest2Time .\n" +
                    "                   FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr10.getStartTime())+"\")<?invest2Time && ?invest2Time < xsd:dateTime(\""+DATE_FORMAT.format(cr10.getEndTime())+"\")) " +
                    "        }\n" +
                    "    }\n" +
                    "    \n" +
                    "    # Union Size\n" +
                    "    {\n" +
                    "        SELECT (COUNT(DISTINCT ?company1) AS ?unionSize) WHERE {\n" +
                    "            VALUES (?person1 ?person2) {\n" +
                    "        (person:"+ cr10.getPid1() +" person:"+ cr10.getPid2() +")\n" +
                    "    }\n" +
                    "            {\n" +
                    "                << ?person1 ex:invest ?company1 >> ex:occurrences ?invest1 .\n" +
                    "                ?invest1 ex:createTime ?invest1Time .\n" +
                    "                   FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr10.getStartTime())+"\")<?invest1Time && ?invest1Time < xsd:dateTime(\""+DATE_FORMAT.format(cr10.getEndTime())+"\")) " +
                    "            }\n" +
                    "            UNION\n" +
                    "            {\n" +
                    "                << ?person2 ex:invest ?company1 >> ex:occurrences ?invest2 .\n" +
                    "                ?invest2 ex:createTime ?invest2Time .\n" +
                    "                   FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr10.getStartTime())+"\")<?invest2Time && ?invest2Time < xsd:dateTime(\""+DATE_FORMAT.format(cr10.getEndTime())+"\")) " +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead10Result> complexRead10Results = null;
            try {
                complexRead10Results = cr10.deserializeResult(result);
                resultReporter.report(complexRead10Results.size(), complexRead10Results, cr10);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr10);
                resultReporter.report(0, new ArrayList<>(), cr10);
            }
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr11.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr11.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr11.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr11.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX path: <http://www.ontotext.com/path#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX person: <http://example.org/Person/> " +
                    "\n" +
                    "SELECT (SUM(?loanAmount) AS ?sumLoanAmount) (COUNT(?loan) AS ?numLoans) WHERE {\n" +
                    "  # Define the starting and ending accounts\n" +
                    "  VALUES (?src) {\n" +
                    "    (person:"+cr11.getId()+ ")\n" +
                    "  }\n" +
                    "<<?src ex:guarantee ?dst>> ex:occurrences ?occurrence ." +
                    "    OPTIONAL{\n" +
                    "  SERVICE path:search {\n" +
                    "        <urn:path> path:findPath path:allPaths ;\n" +
                    "                   path:sourceNode ?src ;\n" +
                    "                   path:destinationNode ?dst ;\n" +
                    "                   path:minPathLength 1 ." +
                    "    }\n" +
                    "  }\n" +
                    " <<?dst ex:apply ?loan>> ex:occurrences ?oc ." +
                    " ?loan ex:loanAmount ?loanAmount ." +
                    "}\n";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead11Result> complexRead11Results = null;
            try {
                complexRead11Results = cr11.deserializeResult(result);
                resultReporter.report(complexRead11Results.size(), complexRead11Results, cr11);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr11);
                resultReporter.report(0, new ArrayList<>(), cr11);
            }
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(cr12.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr12.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr12.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr12.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX person: <http://example.org/Person/> " +
                    "SELECT ?compAccountId (round(1000 * sum(?transferAmount))/1000 AS ?sumEdge2Amount) WHERE {" +
                    "BIND(person:"+ cr12.getId()+" AS ?person)" +
                    "<< ?person ex:own ?account >> ex:occurrences ?personOwnEdge ." +
                    "<< ?account ex:transfer ?companyAccount >> ex:occurrences ?transferEdge ." +
                    "?company a ex:Company ." +
                    "<< ?company ex:own ?companyAccount >> ex:occurrences ?companyOwnEdge ." +
                    "?transferEdge  ex:createTime ?createTime ;" +
                    "               ex:amount ?transferAmount ." +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr12.getStartTime())+"\")<?createTime && ?createTime<xsd:dateTime(\"" + DATE_FORMAT.format(cr12.getEndTime()) +"\")) " +
                    "BIND(xsd:long(STRAFTER(STR(?companyAccount), \"http://example.org/Account/\")) AS ?compAccountId) " +
                    "}" +
                    "GROUP BY ?compAccountId " +
                    "ORDER BY DESC (?sumEdge2Amount) ASC(?compAccountId)";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead12Result> complexRead12Results = null;
            try {
                complexRead12Results = cr12.deserializeResult(result);
                resultReporter.report(complexRead12Results.size(), complexRead12Results, cr12);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + cr12);
                resultReporter.report(0, new ArrayList<>(), cr12);
            }
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(sr1.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr1.getId());

            String queryString = "PREFIX account: <http://example.org/Account/>\n" +
                    "            PREFIX ex: <http://example.org/>\n" +
                    "            SELECT ?createTime ?isBlocked ?type\n" +
                    "            WHERE {\n" +
                    "                BIND(account:"+sr1.getId()+" AS ?account)\n" +
                    "                ?account ex:createTime ?createTime ;\n" +
                    "                         ex:isBlocked ?isBlocked ;\n" +
                    "                         ex:accountType ?type .        \n" +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();

            String result = client.execute(queryString);
            List<SimpleRead1Result> simpleRead1Results = null;
            try {
                simpleRead1Results = sr1.deserializeResult(result);
                resultReporter.report(simpleRead1Results.size(), simpleRead1Results, sr1);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + sr1);
                resultReporter.report(0, new ArrayList<>(), sr1);
            }

        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(sr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr2.getId());
            queryParams.put("start_time", DATE_FORMAT.format(sr2.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr2.getEndTime()));

            String queryString = "PREFIX account: <http://example.org/Account/> " +
                    "             PREFIX ex: <http://example.org/>" +
                    "             SELECT    (round(1000*MAX(?edge1Amount))/1000 AS ?maxEdge1Amount) " +
                    "                       (round(1000*SUM(DISTINCT ?edge1Amount))/1000 AS ?sumEdge1Amount) " +
                    "                       (COUNT(DISTINCT ?occurrences1) AS ?numEdge1) " +
                    "                       (round(1000*MAX(?edge2Amount))/1000 AS ?maxEdge2Amount) " +
                    "                       (round(1000*SUM(DISTINCT ?edge2Amount))/1000 AS ?sumEdge2Amount) " +
                    "                       (COUNT(DISTINCT ?occurrences2) AS ?numEdge2) " +
                    "             WHERE{" +
                    "                 BIND(account:"+sr2.getId()+" AS ?src)  " +
                    "                 <<?src ex:transfer ?dst1>> ex:occurrences ?occurrences1 ." +
                    "                 ?occurrences1 ex:createTime ?edge1CreateTime ." +
                    "                 ?occurrences1 ex:amount ?edge1Amount . " +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr2.getStartTime())+"\")<?edge1CreateTime && ?edge1CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr2.getEndTime()) +"\")) " +
                    "                 <<?dst2 ex:transfer ?src>> ex:occurrences ?occurrences2 ." +
                    "                 ?occurrences2 ex:createTime ?edge2CreateTime ." +
                    "                 ?occurrences2 ex:amount ?edge2Amount . " +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr2.getStartTime())+"\")<?edge2CreateTime && ?edge2CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr2.getEndTime()) +"\")) " +
                    "}";
            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead2Result> simpleRead2Results = null;
            try {
                simpleRead2Results = sr2.deserializeResult(result);
                resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + sr2);
                resultReporter.report(0, new ArrayList<>(), sr2);
            }

        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(sr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr3.getId());
            queryParams.put("threshold", sr3.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr3.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr3.getEndTime()));

            String queryString = "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX ex: <http://example.org/> " +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> " +

                    // Erste Teilabfrage: Zählt alle Transfers zu ?dst
                    "SELECT (IF(COUNT(?src1) > 0, round(1000 * (COUNT(DISTINCT ?occurrences2) / COUNT(DISTINCT ?occurrences1))) / 1000, -1) AS ?blockRatio) " +
                    "WHERE { " +
                    "  BIND(account:" + sr3.getId() + " AS ?dst) " +

                    // Teil 1: Alle Transfers zu ?dst
                    "  <<?src1 ex:transfer ?dst>> ex:occurrences ?occurrences1 ." +
                    "  ?occurrences1 ex:createTime ?edge1CreateTime . " +

                    // Teil 2: Geblockte Transfers zu ?dst
                    "  OPTIONAL { " +
                    "    ?src2 ex:isBlocked true . " +
                    "    <<?src2 ex:transfer ?dst>> ex:occurrences ?occurrences2 ." +
                    "    ?occurrences2 ex:createTime ?edge2CreateTime ; " +
                    "                 ex:amount ?edge2Amount . " +
                    "    FILTER(xsd:dateTime(\"" + DATE_FORMAT.format(sr3.getStartTime()) + "\") < ?edge2CreateTime && " +
                    "           ?edge2CreateTime < xsd:dateTime(\"" + DATE_FORMAT.format(sr3.getEndTime()) + "\")) " +
                    "    FILTER(?edge2Amount > " + sr3.getThreshold() + ") " +
                    "  } " +
                    "}";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead3Result> simpleRead3Results = null;
            try {
                simpleRead3Results = sr3.deserializeResult(result);
                resultReporter.report(simpleRead3Results.size(), simpleRead3Results, sr3);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + sr3);
                resultReporter.report(0, new ArrayList<>(), sr3);
            }
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(sr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr4.getId());
            queryParams.put("threshold", sr4.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr4.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr4.getEndTime()));

            String queryString = "PREFIX account: <http://example.org/Account/> " +
                    "             PREFIX ex: <http://example.org/>" +
                    "             SELECT ?dstId " +
                    "(COUNT(?occurrences1) AS ?numEdges) " +
                    "(round(1000*SUM(?edge1Amount))/1000 AS ?sumAmount)" +
                    "             WHERE{" +
                    "                 BIND(account:"+sr4.getId()+" AS ?src)  " +
                    "                 <<?src ex:transfer ?dst>> ex:occurrences ?occurrences1 ." +
                    "                 ?occurrences1 ex:createTime ?edge1CreateTime ." +
                    "                 ?occurrences1 ex:amount ?edge1Amount . " +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr4.getStartTime())+"\")<?edge1CreateTime && ?edge1CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr4.getEndTime()) +"\")) " +
                    "FILTER(?edge1Amount>"+sr4.getThreshold()+") " +
                    "BIND(STRAFTER(STR(?dst), \"http://example.org/Account/\") AS ?dstId) " +
                    "} GROUP BY ?dstId "+
                    "ORDER BY DESC(?sumAmount) ASC(?dstId)";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead4Result> simpleRead4Results = null;
            try {
                simpleRead4Results = sr4.deserializeResult(result);
                resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + sr4);
                resultReporter.report(0, new ArrayList<>(), sr4);
            }
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(sr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr5.getId());
            queryParams.put("threshold", sr5.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr5.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr5.getEndTime()));


            String queryString = "PREFIX account: <http://example.org/Account/> " +
                    "             PREFIX ex: <http://example.org/>" +
                    "             SELECT ?srcId " +
                    "(COUNT(?occurrences1) AS ?numEdges) " +
                    "(round(1000*SUM(?edge1Amount))/1000 AS ?sumAmount)" +
                    "             WHERE{" +
                    "                 BIND(account:"+sr5.getId()+" AS ?dst)  " +
                    "                 <<?src ex:transfer ?dst>> ex:occurrences ?occurrences1 ." +
                    "                 ?occurrences1 ex:createTime ?edge1CreateTime ." +
                    "                 ?occurrences1 ex:amount ?edge1Amount . " +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr5.getStartTime())+"\")<?edge1CreateTime && ?edge1CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr5.getEndTime()) +"\")) " +
                    "FILTER(?edge1Amount>"+sr5.getThreshold()+") " +
                    "BIND(STRAFTER(STR(?src), \"http://example.org/Account/\") AS ?srcId) " +
                    "} GROUP BY ?srcId "+
                    "ORDER BY DESC(?sumAmount) ASC(?srcId)";
            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead5Result> simpleRead5Results = null;
            try {
                simpleRead5Results = sr5.deserializeResult(result);
                resultReporter.report(simpleRead5Results.size(), simpleRead5Results, sr5);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + sr5);
                resultReporter.report(0, new ArrayList<>(), sr5);
            }
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(sr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr6.getId());
            queryParams.put("start_time", DATE_FORMAT.format(sr6.getStartTime().getTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr6.getEndTime().getTime()));

            String queryString = "PREFIX account: <http://example.org/Account/> " +
                    "             PREFIX ex: <http://example.org/>" +
                    "             SELECT DISTINCT ?dstId" +      //EIGENTLICH COLLECT
                    "             WHERE{" +
                    "                 BIND(account:"+sr6.getId()+" AS ?src)  " +
                    "                 <<?mid ex:transfer ?src>> ex:occurrences ?occurrences1 ." +
                    "                 ?occurrences1 ex:createTime ?edge1CreateTime ." +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr6.getStartTime())+"\")<?edge1CreateTime && ?edge1CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr6.getEndTime()) +"\")) " +
                    "                 <<?mid ex:transfer ?dst>> ex:occurrences ?occurrences2 ." +
                    "                 ?dst ex:isBlocked true ." +
                    "                 ?occurrences2 ex:createTime ?edge2CreateTime ." +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr6.getStartTime())+"\")<?edge2CreateTime && ?edge2CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr6.getEndTime()) +"\")) " +
                    "FILTER(?src != ?dst) " +
                    "BIND(xsd:long(STRAFTER(STR(?dst), \"http://example.org/Account/\")) AS ?dstId) " +
                    "} "+
                    "ORDER BY ASC(?dstId)";
            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead6Result> simpleRead6Results = null;
            try {
                simpleRead6Results = sr6.deserializeResult(result);
                resultReporter.report(simpleRead6Results.size(), simpleRead6Results, sr6);
            } catch (IOException e) {
                GraphDb.logger.warn(e.getMessage() + "\n" + sr6);
                resultReporter.report(0, new ArrayList<>(), sr6);
            }
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w1.toString());

            //Add a person Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "INSERT DATA{" +
                    "person:" + w1.getPersonId() + " a ex:Person ;" +
                    "ex:personName \"" + w1.getPersonName() + "\" ;" +
                    "ex:isBlocked \"" + w1.getIsBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w1);
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w2.toString());

            //Add a Company Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "INSERT DATA{" +
                    "company:" + w2.getCompanyId() + " a ex:Company ;" +
                    "ex:companyName \"" + w2.getCompanyName() + "\" ;" +
                    "ex:isBlocked \"" + w2.getIsBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w2);
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w3.toString());

            //Add a Medium Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX medium: <http://example.org/Medium/> " +
                    "INSERT DATA{" +
                    "medium:" + w3.getMediumId() + " a ex:Medium ;" +
                    "ex:mediumType \"" + w3.getMediumType() + "\" ;" +
                    "ex:isBlocked \"" + w3.getIsBlocked()+ "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";
            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w3);
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w4.toString());

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                     account:" + w4.getAccountId() + " a ex:Account ;" +
                    "                     ex:accountType \"" + w4.getAccountType() + "\" ;" +
                    "                     ex:isBlocked \"" + w4.getAccountBlocked()+ "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w4.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                    << person:" + w4.getPersonId() + " ex:own account:" + w4.getAccountId() + " >> ex:occurrences [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w4.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w4);
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w5.toString());

            //Add an Account Node owned by Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                     account:" + w5.getAccountId() + " a ex:Account ;" +
                    "                     ex:accountType \"" + w5.getAccountType() + "\" ;" +
                    "                     ex:isBlocked \"" + w5.getAccountBlocked()+ "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w5.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                    << company:" + w5.getCompanyId() + " ex:own account:" + w5.getAccountId() + " >> ex:occurrences [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w5.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w5);
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w6.toString());

            //Add Loan applied by Person

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                     loan:" + w6.getLoanId() + " a ex:Account ;" +
                    "                     ex:balance \"" + w6.getBalance() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:loanAmount \"" + w6.getLoanAmount() +"\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w6.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                    << person:" + w6.getPersonId() + " ex:apply loan:" + w6.getLoanId() + " >> ex:occurrences [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w6.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w6);
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w7.toString());

            //Add Loan applied by Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                     loan:" + w7.getLoanId() + " a ex:Account ;" +
                    "                     ex:balance \"" + w7.getBalance() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:loanAmount \"" + w7.getLoanAmount() +"\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w7.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                    << company:" + w7.getCompanyId() + " ex:apply loan:" + w7.getLoanId() + " >> ex:occurrences [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w7.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w7);
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w8.toString());

            //Add Invest Between Person And Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                    << person:" + w8.getPersonId() + " ex:invest company:" + w8.getCompanyId() + " >> ex:occurrences [" +
                    "                     ex:ratio \"" + w8.getRatio() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w8.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w8);
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w9.toString());

            //Add Invest Between Company And Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                    << company:" + w9.getCompanyId1() + " ex:invest company:" + w9.getCompanyId2() + " >> ex:occurrences [" +
                    "                     ex:ratio \"" + w9.getRatio() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w9.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w9);
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w10.toString());

            //Add Guarantee Between Persons
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId1", w10.getPersonId1());
            queryParams.put("personId2", w10.getPersonId2());
            queryParams.put("time", DATE_FORMAT.format(w10.getTime()));

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                    << person:" + w10.getPersonId1() + " ex:guarantee ex:person:" + w10.getPersonId2() + " >> ex:occurrences [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w10.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w10);
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w11.toString());

            //Add Guarantee Between Companies

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                    << company:" + w11.getCompanyId1() + " ex:CompanyGuaranteeCompany company:" + w11.getCompanyId2() + " >> ex:occurrences [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w11.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w11);
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w12.toString());

            //Add Transfer Between Accounts

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "            INSERT DATA{ " +
                    "                    << account:" + w12.getAccountId1() + " ex:transfer account:" + w12.getAccountId2() + " >> ex:occurrences [" +
                    "                     ex:amount \"" + w12.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w12.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w12);
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w13.toString());

            //Add Withdraw Between Accounts

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "            INSERT DATA{ " +
                    "                    << account:" + w13.getAccountId1() + " ex:withdraw account:" + w13.getAccountId2() + " >> ex:occurrences [" +
                    "                     ex:amount \"" + w13.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w13.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w13);
        }
    }

    public static class Write14Handler implements OperationHandler<Write14, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write14 w14, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w14.toString());

            //Add Repay Between Account And Loan

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "            INSERT DATA{ " +
                    "                    << account:" + w14.getAccountId() + " ex:repay loan:" + w14.getLoanId() + " >> ex:occurrences [" +
                    "                     ex:amount \"" + w14.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w14.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w14);
        }
    }

    public static class Write15Handler implements OperationHandler<Write15, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write15 w15, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w15.toString());

            //Add Deposit Between Loan And Account

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "            INSERT DATA{ " +
                    "                    << loan:" + w15.getLoanId() + " ex:deposit account:" + w15.getAccountId() + " >> ex:occurrences [" +
                    "                     ex:amount \"" + w15.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w15.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w15);
        }
    }

    public static class Write16Handler implements OperationHandler<Write16, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write16 w16, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w16.toString());

            //Account signed in with Medium
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w16.getAccountId());
            queryParams.put("mediumId", w16.getMediumId());
            queryParams.put("time", DATE_FORMAT.format(w16.getTime()));

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX medium: <http://example.org/Medium/> " +
                    "            INSERT DATA{ " +
                    "                    << medium:" + w16.getMediumId() + " ex:signIn account:" + w16.getAccountId() + " >> ex:occurrences [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w16.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w16);
        }
    }

    public static class Write17Handler implements OperationHandler<Write17, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write17 w17, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w17.toString());

            //Remove an Account
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w17.getAccountId());

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX account: <http://example.org/Account/> " +
                    "\n" +
                    "# Löschen des Accounts und aller zugehörigen Tripel\n" +
                    "DELETE {\n" +
                    "  # Löscht alle Tripel, in denen der Account als Subjekt oder Objekt beteiligt ist\n" +
                    "  ?s ?p ?o .\n" +
                    "  # Löscht alle Tripel, die den Account in den Kanten `own`, `transfer`, `withdraw`, `repay`, `deposit`, `signIn` betreffen\n" +
                    "  ?s ?p ?account .\n" +
                    "  ?account ?p2 ?o2 .\n" +
                    "  # Löscht alle Tripel, die `Loan`-Objekte betreffen, die mit dem Account verbunden sind\n" +
                    "  ?loan ex:deposit ?account .\n" +
                    "  ?account ex:repay ?loan .\n" +
                    "  <<?account ?a ?b>> ex:occurrences ?occurrence1 .\n" +
                    "  <<?c ?d ?account>> ex:occurrences ?occurrence2 .\n" +
                    "}\n" +
                    "WHERE {\n" +
                    "  # Account-ID binden\n" +
                    "  BIND(account:"+w17.getAccountId()+" AS ?account)\n" +
                    "  \n" +
                    "  # Löschen der Tripel, in denen der Account als Subjekt auftritt\n" +
                    "  {\n" +
                    "    ?account ?p ?o .\n" +
                    "  }\n" +
                    "  UNION\n" +
                    "  # Löschen der Tripel, in denen der Account als Objekt auftritt\n" +
                    "  {\n" +
                    "    ?s ?p ?account .\n" +
                    "  }\n" +
                    "  UNION\n" +
                    "  # Löschen der Kanten wie `own`, `transfer`, `withdraw`, `repay`, `deposit`, `signIn`\n" +
                    "  {\n" +
                    "    ?s ?p ?account .\n" +
                    "    ?account ?p2 ?o2 .\n" +
                    "  }\n" +
                    "  UNION\n" +
                    "  # Löschen von Loan-Objekten, die mit dem Account verbunden sind\n" +
                    "  {\n" +
                    "    ?account ex:repay ?loan .\n" +
                    "    ?loan ex:deposit ?account .\n" +
                    "  }\n" +
                    "    UNION{\n" +
                    "  \t\t<<?account ?a ?b>> ex:occurrences ?occurrence1 .\n" +
                    "    }\n" +
                    "    UNION{\n" +
                    "  \t\t<<?c ?d ?account>> ex:occurrences ?occurrence2 .\n" +
                    "    }\n" +
                    "}\n";


            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w17);
        }
    }

    public static class Write18Handler implements OperationHandler<Write18, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write18 w18, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w18.toString());

            //Block an Account of high risk
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w18.getAccountId());

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "            INSERT DATA{ " +
                    "                     account:"+w18.getAccountId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w18);
        }
    }

    public static class Write19Handler implements OperationHandler<Write19, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write19 w19, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(w19.toString());

            //Block a Person of high risk
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w19.getPersonId());

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                     person:"+w19.getPersonId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w19);
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, GraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(rw1.toString());
            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();

            String simpleRead1StringSrc = "PREFIX account: <http://example.org/Account/>\n" +
                    "            PREFIX ex: <http://example.org/>\n" +
                    "            SELECT ?createTime ?isBlocked ?type\n" +
                    "            WHERE {\n" +
                    "                BIND(account:"+rw1.getSrcId()+" AS ?account)\n" +
                    "                ?account ex:createTime ?createTime ;\n" +
                    "                         ex:isBlocked ?isBlocked ;\n" +
                    "                         ex:accountType ?type .        \n" +
                    "            }";
            String simpleRead1StringDst = "PREFIX account: <http://example.org/Account/>\n" +
                    "            PREFIX ex: <http://example.org/>\n" +
                    "            SELECT ?createTime ?isBlocked ?type\n" +
                    "            WHERE {\n" +
                    "                BIND(account:"+rw1.getDstId()+" AS ?account)\n" +
                    "                ?account ex:createTime ?createTime ;\n" +
                    "                         ex:isBlocked ?isBlocked ;\n" +
                    "                         ex:accountType ?type .        \n" +
                    "            }";

            String resultSrc = client.execute(simpleRead1StringSrc);
            String resultDst = client.execute(simpleRead1StringDst);
            try {
                SimpleRead1Result[] simpleRead1SrcResults = new ObjectMapper().readValue(resultSrc, SimpleRead1Result[].class);
                SimpleRead1Result[] simpleRead1DstResults = new ObjectMapper().readValue(resultDst, SimpleRead1Result[].class);
                if(simpleRead1SrcResults.length>0 && simpleRead1SrcResults[0].getIsBlocked() || simpleRead1DstResults.length>0 && simpleRead1DstResults[0].getIsBlocked()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                    return;
                }
                String write12String = "PREFIX ex: <http://example.org/> " +
                        "PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                    << account:" + rw1.getSrcId() + " ex:transfer account:" + rw1.getDstId() + " >> ex:occurrences [" +
                        "                     ex:amount \"" + rw1.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                        "                     ex:createTime \""+ DATE_FORMAT.format(rw1.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                        "            }";


                RepositoryConnection connection = client.startTransaction(write12String);


                if(!connection.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                    return;
                }

                String complexRead4String = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX account: <http://example.org/Account/> " +
                        "PREFIX ex: <http://example.org/>\n" +
                        "\n" +
                        "SELECT \n" +
                        "?otherId " +
                        "   (round(1000*MAX(?amount2))/1000 AS ?maxEdge2Amount)" +
                        "   (round(1000*SUM(DISTINCT ?amount2))/1000 AS ?sumEdge2Amount)" +
                        "   (COUNT(DISTINCT ?occurrence2) AS ?numEdge2)" +
                        "   (round(1000*MAX(?amount3))/1000 AS ?maxEdge3Amount) " +
                        "   (round(1000*SUM(DISTINCT ?amount3))/1000 AS ?sumEdge3Amount) " +
                        "   (COUNT(DISTINCT ?occurrence3) AS ?numEdge3)" +
                        "WHERE {\n" +
                        "    VALUES (?src ?dst) {\n" +
                        "        (account:"+rw1.getSrcId()+" account:"+rw1.getDstId()+")\n" +
                        "    }\n" +
                        "    \n" +
                        "    # Step 1: Check if src transferred money to dst within the time window\n" +
                        "    <<?src ex:transfer ?dst>> ex:occurrences ?occurrence1 .\n" +
                        "    ?occurrence1 ex:createTime ?createTime1 .\n" +
                        "    ?occurrence1 ex:amount ?amount1 .\n" +
                        "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(rw1.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(rw1.getEndTime())+"\"))\n" +
                        "    \n" +
                        "    # Step 2: Find all other accounts that received money from dst and transferred money to src\n" +
                        "    <<?dst ex:transfer ?other>> ex:occurrences ?occurrence2 .\n" +
                        "    ?occurrence2 ex:createTime ?createTime2 .\n" +
                        "    ?occurrence2 ex:amount ?amount2 .\n" +
                        "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(rw1.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(rw1.getEndTime())+"\"))\n" +
                        "    \n" +
                        "    <<?other ex:transfer ?src>> ex:occurrences ?occurrence3 .\n" +
                        "    ?occurrence3 ex:createTime ?createTime3 .\n" +
                        "    ?occurrence3 ex:amount ?amount3 .\n" +
                        "    FILTER(?createTime3 > xsd:dateTime(\""+DATE_FORMAT.format(rw1.getStartTime())+"\") && ?createTime3 < xsd:dateTime(\""+DATE_FORMAT.format(rw1.getEndTime())+"\"))\n" +
                        "    \n" +
                        "    BIND(STRAFTER(STR(?other), \"http://example.org/Account\") AS ?otherId)\n" +
                        "}\n" +
                        "GROUP BY ?otherId\n";


                TupleQueryResult result = connection.prepareTupleQuery(complexRead4String).evaluate();
                String resultCr4 = client.resultToString(result);
                ComplexRead4Result[] complexRead4Results = new ObjectMapper().readValue(resultCr4, ComplexRead4Result[].class);
                if (complexRead4Results.length == 0 || complexRead4Results[0].getOtherId() == -1) {
                    if(connection.isOpen()){
                        connection.commit();
                        connection.close();
                    }
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                    return;
                }
                if(connection.isOpen()){
                    connection.rollback();
                    connection.close();
                }

                String write18StringSrc = "PREFIX ex: <http://example.org/> " +
                        "PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                     account:"+rw1.getSrcId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";
                String write18StringDst = "PREFIX ex: <http://example.org/> " +
                        "PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                     account:"+rw1.getDstId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";

                client.executeWrite(write18StringSrc);
                client.executeWrite(write18StringDst);


            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, GraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();

            String simpleRead1StringSrc = "PREFIX account: <http://example.org/Account/>\n" +
                    "            PREFIX ex: <http://example.org/>\n" +
                    "            SELECT ?createTime ?isBlocked ?type\n" +
                    "            WHERE {\n" +
                    "                BIND(account:"+rw2.getSrcId()+" AS ?account)\n" +
                    "                ?account ex:createTime ?createTime ;\n" +
                    "                         ex:isBlocked ?isBlocked ;\n" +
                    "                         ex:accountType ?type .        \n" +
                    "            }";
            String simpleRead1StringDst = "PREFIX account: <http://example.org/Account/>\n" +
                    "            PREFIX ex: <http://example.org/>\n" +
                    "            SELECT ?createTime ?isBlocked ?type\n" +
                    "            WHERE {\n" +
                    "                BIND(account:"+rw2.getDstId()+" AS ?account)\n" +
                    "                ?account ex:createTime ?createTime ;\n" +
                    "                         ex:isBlocked ?isBlocked ;\n" +
                    "                         ex:accountType ?type .        \n" +
                    "            }";

            String resultSrc = client.execute(simpleRead1StringSrc);
            String resultDst = client.execute(simpleRead1StringDst);
            try {
                SimpleRead1Result[] simpleRead1SrcResults = new ObjectMapper().readValue(resultSrc, SimpleRead1Result[].class);
                SimpleRead1Result[] simpleRead1DstResults = new ObjectMapper().readValue(resultDst, SimpleRead1Result[].class);
                if(simpleRead1SrcResults.length>0 && simpleRead1SrcResults[0].getIsBlocked() || simpleRead1DstResults.length>0 && simpleRead1DstResults[0].getIsBlocked()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                    return;
                }
                String write12String = "PREFIX ex: <http://example.org/> " +
                        "PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                    << account:" + rw2.getSrcId() + " ex:transfer ex:account:" + rw2.getDstId() + " >> ex:occurrences [" +
                        "                     ex:amount \"" + rw2.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                        "                     ex:createTime \""+ DATE_FORMAT.format(rw2.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                        "            }";


                RepositoryConnection connection = client.startTransaction(write12String);

                if(!connection.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                    return;
                }
                List<Float> ratios = new ArrayList<>();

                for(Long id: new Long[]{rw2.getSrcId(), rw2.getDstId()}) {


                    String complexRead7String = "PREFIX ex: <http://example.org/>\n" +
                            "PREFIX account: <http://example.org/Account/> " +
                            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                            "\n" +
                            "SELECT (COUNT(?src) AS ?numSrc) \n" +
                            "       (COUNT(?dst) AS ?numDst) \n" +
                            "       (IF(SUM(?edge2Amount) > 0, ROUND(1000 * (SUM(?edge1Amount) / SUM(?edge2Amount))) / 1000, 0) AS ?inOutRatio) \n" +
                            "WHERE {\n" +
                            "    \n" +
                            "  BIND(account:"+id+" AS ?mid)\n" +
                            "  # Match the source, intermediary, and destination accounts\n" +
                            "  \n" +
                            "  # Match the transfer edges and retrieve their amounts\n" +
                            "  <<?src ex:transfer ?mid>> ex:occurrences ?edge1Occurrence .\n" +
                            "  ?edge1Occurrence ex:amount ?edge1Amount .\n" +
                            "  ?edge1Occurrence ex:createTime ?createTime1 .\n" +
                            "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(rw2.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(rw2.getEndTime())+"\"))\n" +
                            "    FILTER(?edge1Amount > "+rw2.getAmountThreshold()+")\n" +
                            "    \n" +
                            "  <<?mid ex:transfer ?dst>> ex:occurrences ?edge2Occurrence .\n" +
                            "  ?edge2Occurrence ex:amount ?edge2Amount .\n" +
                            "  ?edge2Occurrence ex:createTime ?createTime2 .\n" +
                            "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(rw2.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(rw2.getEndTime())+"\"))\n" +
                            "    FILTER(?edge2Amount > "+rw2.getAmountThreshold()+")\n" +
                            "}\n" +
                            "\n" +
                            "GROUP BY ?src ?dst\n" +
                            "ORDER BY DESC(?inOutRatio)";


                    TupleQueryResult result = connection.prepareTupleQuery(complexRead7String).evaluate();
                    String resultCr7 = client.resultToString(result);
                    ComplexRead7Result[] complexRead7Results = new ObjectMapper().readValue(resultCr7, ComplexRead7Result[].class);
                    if(complexRead7Results.length>0) ratios.add(complexRead7Results[0].getInOutRatio());
                }

                if (ratios.size()==2 && (ratios.get(0) <= rw2.getRatioThreshold() || ratios.get(1) <= rw2.getRatioThreshold())) {
                    if(connection.isOpen()){
                        connection.commit();
                        connection.close();
                    }
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                    return;
                }
                if(connection.isOpen()){
                    connection.rollback();
                    connection.close();
                }


                String write18StringSrc = "PREFIX ex: <http://example.org/> " +
                        "PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                     account:"+rw2.getSrcId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";
                String write18StringDst = "PREFIX ex: <http://example.org/> " +
                        "PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                     account:"+rw2.getDstId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";

                client.executeWrite(write18StringSrc);
                client.executeWrite(write18StringDst);


            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, GraphDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDb.logger.info(rw3.toString());

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();

            String simpleReadStringSrc = "PREFIX person: <http://example.org/Person/>\n" +
                    "            PREFIX ex: <http://example.org/>\n" +
                    "            SELECT ?createTime ?isBlocked ?type\n" +
                    "            WHERE {\n" +
                    "                BIND(person:"+rw3.getSrcId()+" AS ?person)\n" +
                    "                ?person ex:createTime ?createTime ;\n" +
                    "                         ex:isBlocked ?isBlocked ;\n" +
                    "                         ex:null ?type .        \n" +
                    "            }";
            String simpleReadStringDst = "PREFIX person: <http://example.org/person/>\n" +
                    "            PREFIX ex: <http://example.org/>\n" +
                    "            SELECT ?createTime ?isBlocked ?type\n" +
                    "            WHERE {\n" +
                    "                BIND(person:"+rw3.getDstId()+" AS ?person)\n" +
                    "                ?person ex:createTime ?createTime ;\n" +
                    "                         ex:isBlocked ?isBlocked ;\n" +
                    "                         ex:null ?type .        \n" +
                    "            }";

            String resultSrc = client.execute(simpleReadStringSrc);
            String resultDst = client.execute(simpleReadStringDst);
            try {
                SimpleRead1Result[] simpleReadSrcResults = new ObjectMapper().readValue(resultSrc, SimpleRead1Result[].class);
                SimpleRead1Result[] simpleReadDstResults = new ObjectMapper().readValue(resultDst, SimpleRead1Result[].class);
                if(simpleReadSrcResults.length>0 && simpleReadSrcResults[0].getIsBlocked() || simpleReadDstResults.length>0 && simpleReadDstResults[0].getIsBlocked()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                    return;
                }
                String write10String = "PREFIX ex: <http://example.org/> " +
                        "PREFIX person: <http://example.org/person/> " +
                        "            INSERT DATA{ " +
                        "                    << person:" + rw3.getSrcId() + " ex:guarantee person:" + rw3.getSrcId() + " >> ex:occurrences [" +
                        "                     ex:createTime \""+ DATE_FORMAT.format(rw3.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                        "            }";

                RepositoryConnection connection = client.startTransaction(write10String);

                if(!connection.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                    return;
                }
                //HIER CR11
                String complexRead11String = "PREFIX ex: <http://example.org/>\n" +
                        "PREFIX path: <http://www.ontotext.com/path#>\n" +
                        "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX person: <http://example.org/Person/> " +
                        "\n" +
                        "SELECT (SUM(?loanAmount) AS ?sumLoanAmount) (COUNT(?loan) AS ?numLoans) WHERE {\n" +
                        "  # Define the starting and ending accounts\n" +
                        "  VALUES (?src) {\n" +
                        "    (person:"+rw3.getSrcId()+ ")\n" +
                        "  }\n" +
                        "<<?src ex:guarantee ?dst>> ex:occurrences ?occurrence ." +
                        "    OPTIONAL{\n" +
                        "  SERVICE path:search {\n" +
                        "        <urn:path> path:findPath path:allPaths ;\n" +
                        "                   path:sourceNode ?src ;\n" +
                        "                   path:destinationNode ?dst ;\n" +
                        "                   path:minPathLength 1 ." +
                        "    }\n" +
                        "  }\n" +
                        " <<?dst ex:apply ?loan>> ex:occurrences ?oc ." +
                        " ?loan ex:loanAmount ?loanAmount ." +
                        "}\n";

                String resultCr11 = client.resultToString(connection.prepareTupleQuery(complexRead11String).evaluate());
                ComplexRead11Result[] complexRead11Results = new ObjectMapper().readValue(resultCr11, ComplexRead11Result[].class);


                if (complexRead11Results.length>0 && complexRead11Results[0].getSumLoanAmount()<=rw3.getThreshold()) {
                    if(connection.isOpen()){
                        connection.commit();
                        connection.close();
                    }
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                    return;
                }
                if(connection.isOpen()){
                    connection.rollback();
                    connection.close();
                }



                String write19StringSrc = "PREFIX ex: <http://example.org/> " +
                        "PREFIX person: <http://example.org/person/> " +
                        "            INSERT DATA{ " +
                        "                     person:"+rw3.getSrcId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";
                String write19StringDst = "PREFIX ex: <http://example.org/> " +
                        "PREFIX person: <http://example.org/person/> " +
                        "            INSERT DATA{ " +
                        "                     person:"+rw3.getDstId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";


                client.executeWrite(write19StringSrc);
                client.executeWrite(write19StringDst);

            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
                resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
        }
    }
}
