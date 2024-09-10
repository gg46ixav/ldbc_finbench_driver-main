package org.ldbcouncil.finbench.impls.dummy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class GraphDbReification extends Db {
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
        //registerOperationHandler(ComplexRead11.class, ComplexRead11Handler.class);
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
        //registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);

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
        public void executeOperation(ComplexRead1 cr1, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr1);

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr1.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr1.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr1.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX account: <http://example.org/Account/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
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

                    "    ?blankNode1 rdf:subject ?startAccount ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?account1 ." +

                    "    ?blankNode2 rdf:subject ?medium ; " +
                    "                rdf:predicate ex:signIn ; " +
                    "                rdf:object ?account1 ; " +
                    "                ex:provenance ?occurrence1 . " +
                    "    ?occurrence1 ex:createTime ?signInTime1 . " +

                    "    FILTER(?signInTime1 > xsd:dateTime(\""+ DATE_FORMAT.format(cr1.getStartTime()) +"\") && ?signInTime1 < xsd:dateTime(\""+ DATE_FORMAT.format(cr1.getEndTime()) + "\"))\n" +
                    "    BIND(1 AS ?accountDistance)\n" +
                    "    BIND(?account1 AS ?otherAccount)\n" +
                    "  } UNION {\n" +
                    "    # Depth 2\n" +

                    "    ?blankNode3 rdf:subject ?startAccount ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?account1 ; " +
                    "                ex:provenance ?occurrence21 . " +
                    "    ?occurrence21 ex:createTime ?transfer21Time . " +

                    "    ?blankNode4 rdf:subject ?account1 ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?account2 ; " +
                    "                ex:provenance ?occurrence22 . " +
                    "    ?occurrence22 ex:createTime ?transfer22Time . " +

                    "        FILTER(?transfer21Time < ?transfer22Time)\n" +

                    "    ?blankNode5 rdf:subject ?medium ; " +
                    "                rdf:predicate ex:signIn ; " +
                    "                rdf:object ?account2 ;" +
                    "                ex:provenance ?occurrence2 . " +
                    "    ?occurrence2 ex:createTime ?signInTime2 . " +

                    "    FILTER(?signInTime2 > xsd:dateTime(\""+DATE_FORMAT.format(cr1.getStartTime())+"\") && ?signInTime2 < xsd:dateTime(\""+DATE_FORMAT.format(cr1.getEndTime())+"\"))\n" +
                    "    BIND(2 AS ?accountDistance)\n" +
                    "    BIND(?account2 AS ?otherAccount)\n" +
                    "  } UNION {\n" +
                    "    # Depth 3\n" +

                    "    ?blankNode6 rdf:subject ?startAccount ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?account1 ;" +
                    "                ex:provenance ?occurrence31 . " +
                    "    ?occurrence31 ex:createTime ?transfer31Time . " +

                    "    ?blankNode7 rdf:subject ?account1 ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?account2 ;" +
                    "                ex:provenance ?occurrence32 . " +
                    "    ?occurrence32 ex:createTime ?transfer32Time . " +

                    "    ?blankNode8 rdf:subject ?account2 ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?account3 ;" +
                    "                ex:provenance ?occurrence33 . " +
                    "    ?occurrence33 ex:createTime ?transfer33Time . " +

                    "        FILTER(?transfer31Time < ?transfer32Time && ?transfer32Time < ?transfer33Time)\n" +

                    "    ?blankNode9 rdf:subject ?medium ; " +
                    "                rdf:predicate ex:signIn ; " +
                    "                rdf:object ?account3 ;" +
                    "                ex:provenance ?occurrence3 . " +
                    "    ?occurrence3 ex:createTime ?signInTime3 . " +

                    "    FILTER(?signInTime3 > xsd:dateTime(\""+DATE_FORMAT.format(cr1.getStartTime())+"\") && ?signInTime3 < xsd:dateTime(\""+DATE_FORMAT.format(cr1.getEndTime())+"\"))\n" +
                    "    BIND(3 AS ?accountDistance)\n" +
                    "    BIND(?account3 AS ?otherAccount)\n" +
                    "  }\n" +
                    "  \n" +
                    "  # Extract IDs\n" +
                    "  BIND(xsd:long(STRAFTER(STR(?otherAccount), \"http://example.org/Account/\")) AS ?otherId)\n" +
                    "  BIND(xsd:long(STRAFTER(STR(?medium), \"http://example.org/Medium/\")) AS ?mediumId)\n" +
                    "} " +
                    "ORDER BY ASC(?accountDistance) ASC(?otherId) ASC(?mediumId)";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead1Result> complexRead1Results = null;
            try {
                complexRead1Results = cr1.deserializeResult(result);
                resultReporter.report(complexRead1Results.size(), complexRead1Results, cr1);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr1);
            }


        }
    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr2.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr2.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr2.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX person: <http://example.org/Person/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "\n" +
                    "SELECT ?otherId ((round(1000 * SUM(DISTINCT ?loanAmount))/1000) AS ?sumLoanAmount) ((round(1000 * SUM(DISTINCT ?loanBalance))/1000) AS ?sumLoanBalance) WHERE {\n" +
                    "  # Define the person and their owned accounts\n" +
                    "    ?blankNode rdf:subject person:"+ cr2.getId() + " ; " +
                    "               rdf:predicate ex:own ; " +
                    "               rdf:object ?startAccount ." +
                    "  # Define the paths for transfers with a depth of 1 to 3\n" +
                    "  {\n" +
                    "    # Depth 1\n" +
                    "    ?blankNode1 rdf:subject ?otherAccount1 ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?startAccount ;" +
                    "                ex:provenance ?occurrence1 ." +
                    "    ?occurrence1 ex:createTime ?transferTime1 . " +
                    "    FILTER(?transferTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    BIND(?otherAccount1 AS ?otherAccount)\n" +
                    "    BIND(1 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 2\n" +
                    "\n" +
                    "    ?blankNode2 rdf:subject ?otherAccount2 ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?otherAccount1 ; " +
                    "                ex:provenance ?occurrence21 . " +
                    "    ?occurrence21 ex:createTime ?transferTime21 . " +
                    "    ?blankNode3 rdf:subject ?otherAccount1 ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?startAccount ; " +
                    "                ex:provenance ?occurrence22 . " +
                    "    ?occurrence22 ex:createTime ?transferTime22 . " +
                    "    FILTER(?transferTime21 > ?transferTime22)\n" +
                    "    FILTER(?transferTime21 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime21 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime22 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime22 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    BIND(?otherAccount2 AS ?otherAccount)\n" +
                    "    BIND(2 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 3\n" +

                    "    ?blankNode4 rdf:subject ?otherAccount3 ;\n" +
                    "                rdf:predicate ex:transfer ;\n" +
                    "                rdf:object ?otherAccount2 ;\n" +
                    "                ex:provenance ?occurrence31 .\n" +
                    "    ?occurrence31 ex:createTime ?transferTime31 .\n" +
                    "    \n" +
                    "    ?blankNode5 rdf:subject ?otherAccount2 ;\n" +
                    "                rdf:predicate ex:transfer ;\n" +
                    "                rdf:object ?otherAccount1 ;\n" +
                    "                ex:provenance ?occurrence32 .\n" +
                    "    ?occurrence32 ex:createTime ?transferTime32 .\n" +
                    "    \n" +
                    "    ?blankNode6 rdf:subject ?otherAccount1 ;\n" +
                    "                rdf:predicate ex:transfer ;\n" +
                    "                rdf:object ?startAccount ;\n" +
                    "                ex:provenance ?occurrence33 .\n" +
                    "    ?occurrence33 ex:createTime ?transferTime33 .\n" +
                    "    FILTER(?transferTime31 > ?transferTime32 && ?transferTime32 > ?transferTime33)\n" +
                    "    FILTER(?transferTime31 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime31 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime32 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime32 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime33 > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?transferTime33 < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "    BIND(?otherAccount3 AS ?otherAccount)\n" +
                    "    BIND(3 AS ?distance)\n" +
                    "  }\n" +
                    "  \n" +
                    "  # Define the loans deposited in the other accounts\n" +

                    "    ?blankNode7 rdf:subject ?loan ;\n" +
                    "                rdf:predicate ex:deposit ;\n" +
                    "                rdf:object ?otherAccount ;\n" +
                    "                ex:provenance ?occurrenceL .\n" +
                    "    ?occurrenceL ex:createTime ?loanTime .\n" +
                    "  FILTER(?loanTime > xsd:dateTime(\""+DATE_FORMAT.format(cr2.getStartTime())+"\") && ?loanTime < xsd:dateTime(\""+DATE_FORMAT.format(cr2.getEndTime())+"\"))\n" +
                    "  \n" +
                    "  # Extract loan amount and balance\n" +
                    "  ?loan ex:loanAmount ?loanAmount ;\n" +
                    "        ex:balance ?loanBalance .\n" +
                    "  \n" +
                    "  # Extract other account ID\n" +
                    "  BIND(xsd:long(STRAFTER(STR(?otherAccount), \"http://example.org/Account/\")) AS ?otherId)\n" +
                    "}\n" +
                    "GROUP BY ?otherId\n" +
                    "ORDER BY DESC(?sumLoanAmount) ASC(?otherId)\n";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead2Result> complexRead2Results = null;
            try {
                complexRead2Results = cr2.deserializeResult(result);
                resultReporter.report(complexRead2Results.size(), complexRead2Results, cr2);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr2);
                resultReporter.report(0, new ArrayList<>(), cr2);
            }
        }
    }

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr3.getId1());
            queryParams.put("id2", cr3.getId2());
            queryParams.put("start_time", DATE_FORMAT.format(cr3.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr3.getEndTime()));

            String queryString = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX account: <http://example.org/Account/>\n" +
                    "                    PREFIX path: <http://www.ontotext.com/path#>\n" +
                    "                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "                    SELECT (?dist1 AS ?shortestPathLength) WHERE {\n" +
                    "                      VALUES (?src ?dst) {\n" +
                    "                        (account:"+cr3.getId1() +" account:" +cr3.getId2()+ ")\n" +
                    "                       }\n" +
                    "                        OPTIONAL{\n" +
                    "                      SERVICE path:search {\n" +
                    "                            <urn:path> path:findPath path:distance ;\n" +
                    "                                       path:sourceNode ?src ;\n" +
                    "                                       path:destinationNode ?dst ;\n" +
                    "                                       path:startNode ?start;\n" +
                    "                                       path:endNode ?end;\n" +
                    "                                       path:distanceBinding ?dist ;\n" +
                    "                                       path:maxPathLength 100 .\n" +
                    "                            SERVICE <urn:path> {\n" +
                    "                ?start ex:transfer ?end .\n" +
                    "                ?blankNode rdf:subject ?start ;\n" +
                    "                rdf:predicate ex:transfer ;\n" +
                    "                        rdf:object ?end ;\n" +
                    "                ex:provenance ?provenance .\n" +
                    "                ?provenance ex:createTime ?createTime .\n" +
                    "                 FILTER(?createTime>xsd:dateTime(\" "+ DATE_FORMAT.format(cr3.getStartTime()) +"\") && ?createTime<xsd:dateTime(\" "+ DATE_FORMAT.format(cr3.getEndTime()) + "\"))\n" +
                    "                   } " +
                    "        }\n" +
                    "                      }" +
                    "                   " +
                    "                        BIND(COALESCE(?dist, -1) AS ?dist1)" +
                    "                    }";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead3Result> complexRead3Results = null;
            try {
                complexRead3Results = cr3.deserializeResult(result);
                resultReporter.report(complexRead3Results.size(), complexRead3Results, cr3);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr3);
                resultReporter.report(0, new ArrayList<>(), cr3);
            }
        }
    }

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr4.getId1());
            queryParams.put("id2", cr4.getId2());
            queryParams.put("start_time", DATE_FORMAT.format(cr4.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr4.getEndTime()));

            String queryString = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX account: <http://example.org/Account/>\n" +
                    "\n" +
                    "SELECT \n" +
                    " ?otherId" +
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
                    "    ?blankNode1 rdf:subject ?src .\n" +
                    "    ?blankNode1 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode1 rdf:object ?dst .\n" +
                    "    ?blankNode1 ex:provenance ?occurrence1 .\n" +
                    "    ?occurrence1 ex:createTime ?createTime1 .\n" +
                    "    ?occurrence1 ex:amount ?amount1 ." +
                    "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr4.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr4.getEndTime())+"\"))\n" +
                    "    \n" +
                    "    # Step 2: Find all other accounts that received money from dst and transferred money to src\n" +
                    "    ?blankNode3 rdf:subject ?dst .\n" +
                    "    ?blankNode3 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode3 rdf:object ?other .\n" +
                    "    ?blankNode3 ex:provenance ?occurrence3 .\n" +
                    "    ?occurrence3 ex:createTime ?createTime3 .\n" +
                    "    ?occurrence3 ex:amount ?amount3 ." +
                    "    FILTER(?createTime3 > xsd:dateTime(\""+DATE_FORMAT.format(cr4.getStartTime())+"\") && ?createTime3 < xsd:dateTime(\""+DATE_FORMAT.format(cr4.getEndTime())+"\"))\n" +
                    "    \n" +
                    "    ?blankNode2 rdf:subject ?other .\n" +
                    "    ?blankNode2 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode2 rdf:object ?src .\n" +
                    "    ?blankNode2 ex:provenance ?occurrence2 .\n" +
                    "    ?occurrence2 ex:createTime ?createTime2 .\n" +
                    "    ?occurrence2 ex:amount ?amount2 ." +
                    "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(cr4.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(cr4.getEndTime())+"\"))\n" +
                    "    \n" +
                    "    BIND(STRAFTER(STR(?other), \"http://example.org/Account/\") AS ?otherId)\n" +
                    "}\n" +
                    "GROUP BY ?otherId\n";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead4Result> complexRead4Results = null;
            try {
                complexRead4Results = cr4.deserializeResult(result);
                resultReporter.report(complexRead4Results.size(), complexRead4Results, cr4);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr4);
                resultReporter.report(0, new ArrayList<>(), cr4);
            }
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr5.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr5.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr5.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX person: <http://example.org/Person/>\n" +
                    "\n" +
                    "SELECT DISTINCT ?startAccountId ?otherAccount1Id ?otherAccount2Id ?otherAccount3Id WHERE {\n" +
                    "  # Define the person and their owned accounts\n" +
                    "    ?blankNode rdf:subject person:"+cr5.getId()+" .\n" +
                    "    ?blankNode rdf:predicate ex:own .\n" +
                    "    ?blankNode rdf:object ?startAccount ." +
                    "  # Define the paths for transfers with a depth of 1 to 3\n" +
                    "  {\n" +
                    "    # Depth 1\n" +
                    "\n" +
                    "    ?blankNode1 rdf:subject ?startAccount .\n" +
                    "    ?blankNode1 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode1 rdf:object ?otherAccount1 .\n" +
                    "    \n" +
                    "    ?blankNode1 ex:provenance ?occurrence1 .\n" +
                    "    ?occurrence1 ex:createTime ?transferTime1 ." +
                    "    FILTER(?transferTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    BIND(STRAFTER(STR(?otherAccount1), \"http://example.org/Account/\") AS ?otherAccount1Id)\n" +
                    "    BIND(STRAFTER(STR(?startAccount), \"http://example.org/Account/\") AS ?startAccountId)\n" +
                    "    BIND(1 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 2\n" +
                    "    ?blankNode2 rdf:subject ?startAccount .\n" +
                    "    ?blankNode2 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode2 rdf:object ?otherAccount21 .\n" +
                    "    \n" +
                    "    ?blankNode2 ex:provenance ?occurrence21 .\n" +
                    "    ?occurrence21 ex:createTime ?transferTime21 .\n" +
                    "    \n" +
                    "    ?blankNode3 rdf:subject ?otherAccount21 .\n" +
                    "    ?blankNode3 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode3 rdf:object ?otherAccount22 .\n" +
                    "    \n" +
                    "    ?blankNode3 ex:provenance ?occurrence22 .\n" +
                    "    ?occurrence22 ex:createTime ?transferTime22 ." +
                    "    FILTER(?transferTime21 < ?transferTime22)\n" +
                    "    FILTER(?transferTime21 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime21 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    FILTER(?transferTime22 > xsd:dateTime(\""+DATE_FORMAT.format(cr5.getStartTime())+"\") && ?transferTime22 < xsd:dateTime(\""+DATE_FORMAT.format(cr5.getEndTime())+"\"))\n" +
                    "    BIND(STRAFTER(STR(?otherAccount21), \"http://example.org/Account/\") AS ?otherAccount1Id)\n" +
                    "    BIND(STRAFTER(STR(?otherAccount22), \"http://example.org/Account/\") AS ?otherAccount2Id)\n" +
                    "    BIND(STRAFTER(STR(?startAccount), \"http://example.org/Account/\") AS ?startAccountId)\n" +
                    "    BIND(2 AS ?distance)\n" +
                    "  } UNION {\n" +
                    "    # Depth 3\n" +
                    "    ?blankNode4 rdf:subject ?startAccount .\n" +
                    "    ?blankNode4 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode4 rdf:object ?otherAccount31 .\n" +
                    "    \n" +
                    "    ?blankNode4 ex:provenance ?occurrence31 .\n" +
                    "    ?occurrence31 ex:createTime ?transferTime31 .\n" +
                    "    \n" +
                    "    ?blankNode5 rdf:subject ?otherAccount31 .\n" +
                    "    ?blankNode5 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode5 rdf:object ?otherAccount32 .\n" +
                    "    \n" +
                    "    ?blankNode5 ex:provenance ?occurrence32 .\n" +
                    "    ?occurrence32 ex:createTime ?transferTime32 .\n" +
                    "    \n" +
                    "    ?blankNode6 rdf:subject ?otherAccount32 .\n" +
                    "    ?blankNode6 rdf:predicate ex:transfer .\n" +
                    "    ?blankNode6 rdf:object ?otherAccount33 .\n" +
                    "    \n" +
                    "    ?blankNode6 ex:provenance ?occurrence33 .\n" +
                    "    ?occurrence33 ex:createTime ?transferTime33 .\n" +
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


            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
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
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr5);
                resultReporter.report(0, new ArrayList<>(), cr5);
            }
        }
    }

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr6.getId());
            queryParams.put("threshold1", cr6.getThreshold1());
            queryParams.put("threshold2", cr6.getThreshold2());
            queryParams.put("start_time", DATE_FORMAT.format(cr6.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr6.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX account: <http://example.org/Account/>\n" +
                    "\n" +
                    "SELECT ?midId ((round(1000 * SUM(DISTINCT ?edge1Amount))/1000) AS ?sumEdge1Amount) ((round(1000 * SUM(DISTINCT ?edge2Amount))/1000) AS ?sumEdge2Amount) WHERE {\n" +
                    "\n" +
                    "  # Filter for the first edge (transfer)\n" +
                    "  ?blankNode1 rdf:subject ?src1 .\n" +
                    "  ?blankNode1 rdf:predicate ex:transfer .\n" +
                    "  ?blankNode1 rdf:object ?mid .\n" +
                    "  ?blankNode1 ex:provenance ?provenance1 .\n" +
                    "  ?provenance1 ex:createTime ?edge1CreateTime ;\n" +
                    "               ex:amount ?edge1Amount ." +
                    "  FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr6.getStartTime())+"\") < ?edge1CreateTime && ?edge1CreateTime < xsd:dateTime(\""+DATE_FORMAT.format(cr6.getEndTime())+"\"))\n" +
                    "  FILTER(?edge1Amount > "+cr6.getThreshold1()+")\n" +
                    "\n" +
                    "  # Filter for the second edge (withdraw)\n" +
                    "  ?blankNode2 rdf:subject ?mid .\n" +
                    "  ?blankNode2 rdf:predicate ex:withdraw .\n" +
                    "  ?blankNode2 rdf:object account:"+cr6.getId()+" .\n" +
                    "  ?blankNode2 ex:provenance ?provenance2 .\n" +
                    "  ?provenance2 ex:createTime ?edge2CreateTime ;\n" +
                    "              ex:amount ?edge2Amount ." +
                    "  FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr6.getStartTime())+"\") < ?edge2CreateTime && ?edge2CreateTime < xsd:dateTime(\""+DATE_FORMAT.format(cr6.getEndTime())+"\"))\n" +
                    "  FILTER(?edge2Amount > "+cr6.getThreshold2()+")\n" +
                    "  \n" +
                    "  BIND(xsd:long(STRAFTER(STR(?mid), \"http://example.org/Account/\")) AS ?midId)\n" +
                    "} GROUP BY ?midId HAVING (COUNT(?src1) > 3)\n" +
                    "ORDER BY DESC(?sumEdge2Amount) ASC(?midId) \n";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead6Result> complexRead6Results = null;
            try {
                complexRead6Results = cr6.deserializeResult(result);
                resultReporter.report(complexRead6Results.size(), complexRead6Results, cr6);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr6);
                resultReporter.report(0, new ArrayList<>(), cr6);
            }
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr7.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr7.getId());
            queryParams.put("threshold", cr7.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr7.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr7.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX account: <http://example.org/Account/>\n" +
                    "\n" +
                    "SELECT (COUNT(DISTINCT ?src) AS ?numSrc) \n" +
                    "       (COUNT(DISTINCT ?dst) AS ?numDst) \n" +
                    "       (IF(SUM(DISTINCT ?edge2Amount) > 0, ROUND(1000 * (SUM(DISTINCT ?edge1Amount) / SUM(DISTINCT ?edge2Amount))) / 1000, 0) AS ?inOutRatio) \n" +
                    "WHERE {\n" +
                    "    \n" +
                    "  BIND(account:"+cr7.getId()+" AS ?mid)\n" +
                    "  # Match the source, intermediary, and destination accounts\n" +
                    "  \n" +
                    "  # Match the transfer edges and retrieve their amounts\n" +
                    "  ?blankNode1 rdf:subject ?src ;\n" +
                    "              rdf:predicate ex:transfer ;\n" +
                    "              rdf:object ?mid ;\n" +
                    "              ex:provenance ?edge1Occurrence .\n" +
                    "  ?edge1Occurrence ex:amount ?edge1Amount ;\n" +
                    "                   ex:createTime ?createTime1 . " +
                    "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr7.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr7.getEndTime())+"\"))\n" +
                    "    FILTER(?edge1Amount > "+cr7.getThreshold()+")\n" +

                    " ?blankNode2 rdf:subject ?mid .\n" +
                    "  ?blankNode2 rdf:predicate ex:transfer .\n" +
                    "  ?blankNode2 rdf:object ?dst .\n" +
                    "  ?blankNode2 ex:provenance ?edge2Occurrence .\n" +
                    "  ?edge2Occurrence ex:amount ?edge2Amount .\n" +
                    "  ?edge2Occurrence ex:createTime ?createTime2 ." +
                    "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(cr7.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(cr7.getEndTime())+"\"))\n" +
                    "    FILTER(?edge2Amount > "+cr7.getThreshold()+")\n" +
                    "\n" +
                    "}\n" +
                    "\n" +
                    "ORDER BY DESC(?inOutRatio)";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead7Result> complexRead7Results = null;
            try {
                complexRead7Results = cr7.deserializeResult(result);
                resultReporter.report(complexRead7Results.size(), complexRead7Results, cr7);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr7);
                resultReporter.report(0, new ArrayList<>(), cr7);
            }
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr8.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr8.getId());
            queryParams.put("threshold", cr8.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr8.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr8.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX loan: <http://example.org/Loan/>\n" +
                    "\n" +
                    "SELECT ?dstId (ROUND(1000 * (sum(DISTINCT ?lastAmount) / max(?loanAmount))) / 1000 AS ?ratio) (max(?minDistanceFromLoanA) AS ?minDistanceFromLoan) WHERE {\n" +
                    "  # Define the person and their owned accounts\n" +
                    " loan:"+cr8.getId()+" ex:loanAmount ?loanAmount ." +

                    "    ?blankNode1 rdf:subject loan:"+cr8.getId()+" ;\n" +
                    "                rdf:predicate ex:deposit ;\n" +
                    "                rdf:object ?startAccount .\n" +
                    "    \n" +
                    "    ?blankNode1 ex:provenance ?occurrence1 .\n" +
                    "    ?occurrence1 ex:createTime ?depositTime .\n" +
                    "  FILTER(?depositTime > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?depositTime < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "  \n" +
                    "  # Define the paths for transfers or withdraw with a depth of 1 to 3\n" +
                    "  {\n" +
                    "    # Depth 1\n" +
                    "    ?blankNode2 rdf:subject ?startAccount ;\n" +
                    "                rdf:predicate ?predicate ;\n" +
                    "                rdf:object ?otherAccount1 ;\n" +
                    "                ex:provenance ?occurrence2 .\n" +

                    "    ?occurrence2 ex:createTime ?transferTime1 ;\n" +
                    "                ex:amount ?edgeAmount ."+
                    "    FILTER(?predicate = ex:transfer || ?predicate = ex:withdraw) " +
                    "    FILTER(?transferTime1 > xsd:dateTime(\""+DATE_FORMAT.format(cr8.getStartTime())+"\") && ?transferTime1 < xsd:dateTime(\""+DATE_FORMAT.format(cr8.getEndTime())+"\"))\n" +
                    "    FILTER(?edgeAmount > "+cr8.getThreshold()+")\n" +
                    "    BIND(?otherAccount1 AS ?otherAccount)\n" +
                    "    BIND(2 AS ?minDistanceFromLoanA)\n" +
                    "    BIND(?edgeAmount AS ?lastAmount) " + // # Total amount for depth 1"
                    "  } UNION {\n" +
                    "    # Depth 2\n" +
                    "    ?blankNode3 rdf:subject ?startAccount .\n" +
                    "    ?blankNode3 rdf:predicate ?predicate21 .\n" +
                    "    ?blankNode3 rdf:object ?otherAccount21 .\n" +
                    "    ?blankNode3 ex:provenance ?occurrence21 .\n" +
                    "    ?occurrence21 ex:createTime ?transferTime21 .\n" +
                    "    ?occurrence21 ex:amount ?edgeAmount21 .\n" +
                    "    \n" +
                    "    ?blankNode4 rdf:subject ?otherAccount21 .\n" +
                    "    ?blankNode4 rdf:predicate ?predicate22 .\n" +
                    "    ?blankNode4 rdf:object ?otherAccount22 .\n" +
                    "    ?blankNode4 ex:provenance ?occurrence22 .\n" +
                    "    ?occurrence22 ex:createTime ?transferTime22 .\n" +
                    "    ?occurrence22 ex:amount ?edgeAmount22 ." +
                    "    FILTER(?predicate21 = ex:transfer || ?predicate21 = ex:withdraw) " +
                    "    FILTER(?predicate22 = ex:transfer || ?predicate22 = ex:withdraw) " +
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
                    "    ?blankNode5 rdf:subject ?startAccount .\n" +
                    "    ?blankNode5 rdf:predicate ?predicate31 .\n" +
                    "    ?blankNode5 rdf:object ?otherAccount31 .\n" +
                    "    ?blankNode5 ex:provenance ?occurrence31 .\n" +
                    "    ?occurrence31 ex:createTime ?transferTime31 .\n" +
                    "    ?occurrence31 ex:amount ?edgeAmount31 .\n" +
                    "    \n" +
                    "    ?blankNode6 rdf:subject ?otherAccount31 .\n" +
                    "    ?blankNode6 rdf:predicate ?predicate32 .\n" +
                    "    ?blankNode6 rdf:object ?otherAccount32 .\n" +
                    "    \n" +
                    "    ?blankNode6 ex:provenance ?occurrence32 .\n" +
                    "    ?occurrence32 ex:createTime ?transferTime32 .\n" +
                    "    ?occurrence32 ex:amount ?edgeAmount32 .\n" +
                    "    \n" +
                    "    ?blankNode7 rdf:subject ?otherAccount32 .\n" +
                    "    ?blankNode7 rdf:predicate ?predicate33 .\n" +
                    "    ?blankNode7 rdf:object ?otherAccount33 .\n" +
                    "    ?blankNode7 ex:provenance ?occurrence33 .\n" +
                    "    ?occurrence33 ex:createTime ?transferTime33 .\n" +
                    "    ?occurrence33 ex:amount ?edgeAmount33 ." +
                    "    FILTER(?predicate31 = ex:transfer || ?predicate31 = ex:withdraw) " +
                    "    FILTER(?predicate32 = ex:transfer || ?predicate32 = ex:withdraw) " +
                    "    FILTER(?predicate33 = ex:transfer || ?predicate33 = ex:withdraw) " +
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
                    "}\n"+
                    "GROUP BY ?dstId " +
                    "ORDER BY DESC(?minDistanceFromLoan) DESC(?ratio) ASC(?dstId)";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead8Result> complexRead8Results = null;
            try {
                complexRead8Results = cr8.deserializeResult(result);
                resultReporter.report(complexRead8Results.size(), complexRead8Results, cr8);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr8);
                resultReporter.report(0, new ArrayList<>(), cr8);
            }
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr9.toString());

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
                    "        ?blankNode1 rdf:subject ?loan .\n" +
                    "        ?blankNode1 rdf:predicate ex:deposit .\n" +
                    "        ?blankNode1 rdf:object ?startAccount .\n" +
                    "        ?blankNode1 ex:provenance ?edge1 .\n" +
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
                    "        ?blankNode2 rdf:subject ?startAccount .\n" +
                    "        ?blankNode2 rdf:predicate ex:repay .\n" +
                    "        ?blankNode2 rdf:object ?loan .\n" +
                    "        ?blankNode2 ex:provenance ?edge2 .\n" +
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
                    "        ?blankNode3 rdf:subject ?upAccount .\n" +
                    "        ?blankNode3 rdf:predicate ex:transfer .\n" +
                    "        ?blankNode3 rdf:object ?startAccount .\n" +
                    "        ?blankNode3 ex:provenance ?edge3 .\n" +
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
                    "        ?blankNode4 rdf:subject ?startAccount .\n" +
                    "        ?blankNode4 rdf:predicate ex:transfer .\n" +
                    "        ?blankNode4 rdf:object ?downAccount .\n" +
                    "        ?blankNode4 ex:provenance ?edge4 .\n" +
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

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead9Result> complexRead9Results = null;
            try {
                complexRead9Results = cr9.deserializeResult(result);
                resultReporter.report(complexRead9Results.size(), complexRead9Results, cr9);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n " + cr9);
                resultReporter.report(0, new ArrayList<>(), cr9);
            }
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr10.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr10.getPid1());
            queryParams.put("id2", cr10.getPid2());
            queryParams.put("start_time", DATE_FORMAT.format(cr10.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr10.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "PREFIX person: <http://example.org/Person/>\n" +
                    "\n" +
                    "SELECT (IF(BOUND(?unionSize) && ?unionSize > 0, ROUND(1000 * (?intersectionSize / ?unionSize))/1000.0, 0) AS ?jaccardSimilarity) WHERE {  \n" +    //1000.0 da nur float division funktioniert
                    "    \n" +
                    "    # Intersection Size\n" +
                    "    {\n" +
                    "        SELECT (xsd:float(COUNT(DISTINCT ?company)) AS ?intersectionSize) WHERE {\n" +                 //cast to float da nur float division funktioniert
                    "            VALUES (?person1 ?person2) {\n" +
                    "                (person:" + cr10.getPid1() + " person:" + cr10.getPid2() + ")\n" +
                    "            }\n" +
                    "            \n" +
                    "            ?blankNode1 rdf:subject ?person1 .\n" +
                    "            ?blankNode1 rdf:predicate ex:invest .\n" +
                    "            ?blankNode1 rdf:object ?company .\n" +
                    "            \n" +
                    "            ?blankNode1 ex:provenance ?provenance1 .\n" +
                    "            ?provenance1 ex:createTime ?invest1Time .\n" +
                    "            FILTER(xsd:dateTime(\"" + DATE_FORMAT.format(cr10.getStartTime())+ "\")<?invest1Time && ?invest1Time < xsd:dateTime(\"" + DATE_FORMAT.format(cr10.getEndTime()) + "\"))\n" +
                    "            \n" +
                    "            ?blankNode2 rdf:subject ?person2 .\n" +
                    "            ?blankNode2 rdf:predicate ex:invest .\n" +
                    "            ?blankNode2 rdf:object ?company .\n" +
                    "            \n" +
                    "            ?blankNode2 ex:provenance ?provenance2 .\n" +
                    "            ?provenance2 ex:createTime ?invest2Time .\n" +
                    "            FILTER(xsd:dateTime(\"" + DATE_FORMAT.format(cr10.getStartTime()) + "\")<?invest2Time && ?invest2Time < xsd:dateTime(\"" + DATE_FORMAT.format(cr10.getEndTime()) + "\"))\n" +
                    "        }\n" +
                    "    }\n" +
                    "    \n" +
                    "    # Union Size\n" +
                    "    {\n" +
                    "        SELECT (COUNT(DISTINCT ?company1) AS ?unionSize) WHERE {\n" +
                    "            VALUES (?person1 ?person2) {\n" +
                    "                (person:" + cr10.getPid1() + " person:" + cr10.getPid2() + ")\n" +
                    "            }\n" +
                    "            {\n" +
                    "                ?blankNode3 rdf:subject ?person1 .\n" +
                    "                ?blankNode3 rdf:predicate ex:invest .\n" +
                    "                ?blankNode3 rdf:object ?company1 .\n" +
                    "                \n" +
                    "                ?blankNode3 ex:provenance ?provenance3 .\n" +
                    "                ?provenance3 ex:createTime ?invest1Time .\n" +
                    "                FILTER(xsd:dateTime(\"" + DATE_FORMAT.format(cr10.getStartTime()) + "\")<?invest1Time && ?invest1Time < xsd:dateTime(\"" + DATE_FORMAT.format(cr10.getEndTime()) + "\"))\n" +
                    "            }\n" +
                    "            UNION\n" +
                    "            {\n" +
                    "                ?blankNode4 rdf:subject ?person2 .\n" +
                    "                ?blankNode4 rdf:predicate ex:invest .\n" +
                    "                ?blankNode4 rdf:object ?company1 .\n" +
                    "                \n" +
                    "                ?blankNode4 ex:provenance ?provenance4 .\n" +
                    "                ?provenance4 ex:createTime ?invest2Time .\n" +
                    "                FILTER(xsd:dateTime(\"" +DATE_FORMAT.format(cr10.getStartTime())+ "\")<?invest2Time && ?invest2Time < xsd:dateTime(\"" + DATE_FORMAT.format(cr10.getEndTime()) + "\"))\n" +
                    "            }\n" +
                    "        }\n" +
                    "    }\n" +
                    "}\n";


            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead10Result> complexRead10Results = null;
            try {
                complexRead10Results = cr10.deserializeResult(result);
                resultReporter.report(complexRead10Results.size(), complexRead10Results, cr10);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr10);
                resultReporter.report(0, new ArrayList<>(), cr10);
            }
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr11.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr11.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr11.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr11.getEndTime()));

            String queryString = "MATCH path=(p1:Person {personId: $id})-[:guarantee*]->(pX:Person) " +
                    "WHERE all(e IN relationships(path) WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "UNWIND nodes(path)[1..] AS person " +
                    "MATCH (person)-[:apply]->(loan:Loan) " +
                    "RETURN sum(loan.loanAmount) AS sumLoanAmount, count(loan) AS numLoans";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead11Result> complexRead11Results = null;
            try {
                complexRead11Results = cr11.deserializeResult(result);
                resultReporter.report(complexRead11Results.size(), complexRead11Results, cr11);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr11);
                resultReporter.report(0, new ArrayList<>(), cr11);
            }
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, GraphDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(cr12.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr12.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr12.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr12.getEndTime()));

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX person: <http://example.org/Person/>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                    "\n" +
                    "SELECT ?compAccountId (round(1000 * sum(?transferAmount))/1000 AS ?sumEdge2Amount) WHERE {" +
                    "?blankNode rdf:subject person:"+cr12.getId()+" .\n" +
                    "?blankNode rdf:predicate ex:own .\n" +
                    "?blankNode rdf:object ?account .\n" +
                    "\n" +
                    "?blankNode2 rdf:subject ?account .\n" +
                    "?blankNode2 rdf:predicate ex:transfer .\n" +
                    "?blankNode2 rdf:object ?companyAccount .\n" +
                    "?blankNode2 ex:provenance ?provenance .\n" +
                    "?provenance ex:amount ?transferAmount .\n" +
                    "?provenance ex:createTime ?createTime .\n" +
                    "\n" +
                    "?company a ex:Company ." +
                    "?blankNode3 rdf:subject ?company.\n" +
                    "?blankNode3 rdf:predicate ex:own.\n" +
                    "?blankNode3 rdf:object ?companyAccount .\n" +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(cr12.getStartTime())+"\")<?createTime && ?createTime<xsd:dateTime(\"" + DATE_FORMAT.format(cr12.getEndTime()) +"\"))" +
                    "BIND(xsd:long(STRAFTER(STR(?companyAccount), \"http://example.org/Account/\")) AS ?compAccountId)" +
                    "}" +
                    "GROUP BY ?compAccountId " +
                    "ORDER BY DESC (?sumEdge2Amount) ASC(?compAccountId)";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            String result = client.execute(queryString);

            List<ComplexRead12Result> complexRead12Results = null;
            try {
                complexRead12Results = cr12.deserializeResult(result);
                resultReporter.report(complexRead12Results.size(), complexRead12Results, cr12);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + cr12);
                resultReporter.report(0, new ArrayList<>(), cr12);
            }
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(sr1.toString());

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
                GraphDbReification.logger.warn(e.getMessage() + "\n" + sr1);
                resultReporter.report(0, new ArrayList<>(), sr1);
            }

        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(sr2.toString());

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
                    "               ?blankNode rdf:subject ?src ." +
                    "               ?blankNode rdf:predicate ex:transfer .\n" +
                    "               ?blankNode rdf:object ?dst1 .\n" +
                    "               ?blankNode ex:provenance ?occurrences1 ." +
                    "               ?occurrences1 ex:createTime ?edge1CreateTime ." +
                    "               ?occurrences1 ex:amount ?edge1Amount . " +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr2.getStartTime())+"\")<?edge1CreateTime && ?edge1CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr2.getEndTime()) +"\")) " +
                    "               ?blankNode2 rdf:subject ?dst2 ." +
                    "               ?blankNode2 rdf:predicate ex:transfer .\n" +
                    "               ?blankNode2 rdf:object ?src .\n" +
                    "               ?blankNode2 ex:provenance ?occurrences2 ." +
                    "               ?occurrences2 ex:createTime ?edge2CreateTime ." +
                    "               ?occurrences2 ex:amount ?edge2Amount . " +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr2.getStartTime())+"\")<?edge2CreateTime && ?edge2CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr2.getEndTime()) +"\")) " +
                    "}";
            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead2Result> simpleRead2Results = null;
            try {
                simpleRead2Results = sr2.deserializeResult(result);
                resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + sr2);
                resultReporter.report(0, new ArrayList<>(), sr2);
            }

        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(sr3.toString());

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
                    "  ?blankNode rdf:subject ?src1 ; " +
                    "             rdf:predicate ex:transfer ; " +
                    "             rdf:object ?dst ; " +
                    "             ex:provenance ?occurrences1 . " +
                    "  ?occurrences1 ex:createTime ?edge1CreateTime . " +

                    // Teil 2: Geblockte Transfers zu ?dst
                    "  OPTIONAL { " +
                    "    ?src2 ex:isBlocked true . " +
                    "    ?blankNode2 rdf:subject ?src2 ; " +
                    "                rdf:predicate ex:transfer ; " +
                    "                rdf:object ?dst ; " +
                    "                ex:provenance ?occurrences2 . " +
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
                GraphDbReification.logger.warn(e.getMessage() + "\n" + sr3);
                resultReporter.report(0, new ArrayList<>(), sr3);
            }
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(sr4.toString());

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
                    "               ?blankNode rdf:subject ?src ." +
                    "               ?blankNode rdf:predicate ex:transfer .\n" +
                    "               ?blankNode rdf:object ?dst .\n" +
                    "               ?blankNode ex:provenance ?occurrences1 ." +
                    "               ?occurrences1 ex:createTime ?edge1CreateTime ." +
                    "               ?occurrences1 ex:amount ?edge1Amount . " +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr4.getStartTime())+"\")<?edge1CreateTime && ?edge1CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr4.getEndTime()) +"\")) " +
                    "FILTER(?edge1Amount>"+sr4.getThreshold()+") " +
                    "BIND(STRAFTER(STR(?dst), \"http://example.org/Account/\") AS ?dstId) " +
                    "} GROUP BY ?dstId " +
                    "ORDER BY DESC(?sumAmount) ASC(?dstId)";

            GraphDbConnectionState.GraphDbClient client = graphDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead4Result> simpleRead4Results = null;
            try {
                simpleRead4Results = sr4.deserializeResult(result);
                resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
            } catch (IOException e) {
                GraphDbReification.logger.warn(e.getMessage() + "\n" + sr4);
                resultReporter.report(0, new ArrayList<>(), sr4);
            }
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(sr5.toString());

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
                    "               ?blankNode rdf:subject ?src ." +
                    "               ?blankNode rdf:predicate ex:transfer .\n" +
                    "               ?blankNode rdf:object ?dst .\n" +
                    "               ?blankNode ex:provenance ?occurrences1 ." +
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
                GraphDbReification.logger.warn(e.getMessage() + "\n" + sr5);
                resultReporter.report(0, new ArrayList<>(), sr5);
            }
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, GraphDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(sr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr6.getId());
            queryParams.put("start_time", DATE_FORMAT.format(sr6.getStartTime().getTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr6.getEndTime().getTime()));

            String queryString = "PREFIX account: <http://example.org/Account/> " +
                    "             PREFIX ex: <http://example.org/>" +
                    "             SELECT DISTINCT ?dstId" +      //EIGENTLICH COLLECT
                    "             WHERE{" +
                    "                 BIND(account:"+sr6.getId()+" AS ?src)  " +
                    "               ?blankNode rdf:subject ?mid ." +
                    "               ?blankNode rdf:predicate ex:transfer .\n" +
                    "               ?blankNode rdf:object ?src .\n" +
                    "               ?blankNode ex:provenance ?occurrences1 ." +
                    "               ?occurrences1 ex:createTime ?edge1CreateTime ." +
                    "FILTER(xsd:dateTime(\""+DATE_FORMAT.format(sr6.getStartTime())+"\")<?edge1CreateTime && ?edge1CreateTime<xsd:dateTime(\"" + DATE_FORMAT.format(sr6.getEndTime()) +"\")) " +
                    "               ?blankNode2 rdf:subject ?mid ." +
                    "               ?blankNode2 rdf:predicate ex:transfer .\n" +
                    "               ?blankNode2 rdf:object ?dst .\n" +
                    "               ?blankNode2 ex:provenance ?occurrences2 ." +
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
                GraphDbReification.logger.warn(e.getMessage() + "\n" + sr6);
                resultReporter.report(0, new ArrayList<>(), sr6);
            }
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(w1.toString());

            //Add a person Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX person: <http://example.org/Person/> "+
                    "INSERT DATA{" +
                    "person:" + w1.getPersonId() + " a ex:Person ;" +
                    "ex:personName \"" + w1.getPersonName() + "\" ;" +
                    "ex:isBlocked \"" + w1.getIsBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";


            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w1);
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(w2.toString());

            //Add a Company Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "INSERT DATA{" +
                    "company:" + w2.getCompanyId() + " a ex:Company ;" +
                    "ex:companyName \"" + w2.getCompanyName() + "\" ;" +
                    "ex:isBlocked \"" + w2.getIsBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w2);
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(w3.toString());

            //Add a Medium Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX medium: <http://example.org/Medium/> " +
                    "INSERT DATA{" +
                    "medium:" + w3.getMediumId() + " a ex:Medium ;" +
                    "ex:mediumType \"" + w3.getMediumType() + "\" ;" +
                    "ex:isBlocked \"" + w3.getIsBlocked()+ "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w3);
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(w4.toString());

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                     account:" + w4.getAccountId() + " a ex:Account ;" +
                    "                     ex:accountType \"" + w4.getAccountType() + "\" ;" +
                    "                     ex:isBlocked \"" + w4.getAccountBlocked()+ "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w4.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                     [] rdf:subject person:" + w4.getPersonId() + " ; " +
                    "                        rdf:predicate ex:own ; " +
                    "                        rdf:object account:" + w4.getAccountId() + " ; " +
                    "                        ex:provenance [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w4.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";

            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w4);
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, GraphDbConnectionState GraphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(w5.toString());

            //Add an Account Node owned by Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                     account:" + w5.getAccountId() + " a ex:Account ;" +
                    "                     ex:accountType \"" + w5.getAccountType() + "\" ;" +
                    "                     ex:isBlocked \"" + w5.getAccountBlocked()+ "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w5.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                     [] rdf:subject company:" + w5.getCompanyId() + " ; " +
                    "                        rdf:predicate ex:own ; " +
                    "                        rdf:object account:" + w5.getAccountId() + " ; " +
                    "                        ex:provenance [" +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w5.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] . " +
                    "            }";


            GraphDbConnectionState.GraphDbClient client = GraphDbConnectionState.client();
            client.executeWrite(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w5);
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, GraphDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, GraphDbConnectionState graphDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            GraphDbReification.logger.info(w6.toString());

            //Add Loan applied by Person

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                     loan:" + w6.getLoanId() + " a ex:Account ;" +
                    "                     ex:balance \"" + w6.getBalance() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:loanAmount \"" + w6.getLoanAmount() +"\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w6.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                     [] rdf:subject person:" + w6.getPersonId() + " ; " +
                    "                        rdf:predicate ex:apply ; " +
                    "                        rdf:object loan:" + w6.getLoanId() + " ; " +
                    "                        ex:provenance [" +
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
            GraphDbReification.logger.info(w7.toString());

            //Add Loan applied by Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                     loan:" + w7.getLoanId() + " a ex:Account ;" +
                    "                     ex:balance \"" + w7.getBalance() + "\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:loanAmount \"" + w7.getLoanAmount() +"\"^^<http://www.w3.org/2001/XMLSchema#double> ; " +
                    "                     ex:createTime \"" + DATE_FORMAT.format(w7.getTime()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "                    [] rdf:subject company:" + w7.getCompanyId() + " ; " +
                    "                        rdf:predicate ex:apply ; " +
                    "                        rdf:object loan:" + w7.getLoanId() + " ; " +
                    "                        ex:provenance [" +
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
            GraphDbReification.logger.info(w8.toString());

            //Add Invest Between Person And Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject person:" + w8.getPersonId() + " ; " +
                    "                        rdf:predicate ex:invest ; " +
                    "                        rdf:object company:" + w8.getCompanyId() + " ; " +
                    "                        ex:provenance [" +
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
            GraphDbReification.logger.info(w9.toString());

            //Add Invest Between Company And Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject company:" + w9.getCompanyId1() + " ; " +
                    "                        rdf:predicate ex:invest ; " +
                    "                        rdf:object company:" + w9.getCompanyId2() + " ; " +
                    "                        ex:provenance  [" +
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
            GraphDbReification.logger.info(w10.toString());

            //Add Guarantee Between Persons
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId1", w10.getPersonId1());
            queryParams.put("personId2", w10.getPersonId2());
            queryParams.put("time", DATE_FORMAT.format(w10.getTime()));

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject person:" + w10.getPersonId1() + " ; " +
                    "                        rdf:predicate ex:guarantee ; " +
                    "                        rdf:object person:" + w10.getPersonId2() + " ; " +
                    "                        ex:provenance [" +
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
            GraphDbReification.logger.info(w11.toString());

            //Add Guarantee Between Companies

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX company: <http://example.org/Company/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject company:" + w11.getCompanyId1() + " ; " +
                    "                        rdf:predicate ex:guarantee ; " +
                    "                        rdf:object company:" + w11.getCompanyId2() + " ; " +
                    "                        ex:provenance [" +
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
            GraphDbReification.logger.info(w12.toString());

            //Add Transfer Between Accounts

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject account:" + w12.getAccountId1() + " ; " +
                    "                        rdf:predicate ex:transfer ; " +
                    "                        rdf:object account:" + w12.getAccountId2() + " ; " +
                    "                        ex:provenance [" +
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
            GraphDbReification.logger.info(w13.toString());

            //Add Withdraw Between Accounts

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject account:" + w13.getAccountId1() + " ; " +
                    "                        rdf:predicate ex:withdraw ; " +
                    "                        rdf:object account:" + w13.getAccountId2() + " ; " +
                    "                        ex:provenance  [" +
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
            GraphDbReification.logger.info(w14.toString());

            //Add Repay Between Account And Loan

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject account:" + w14.getAccountId() + " ; " +
                    "                        rdf:predicate ex:repay ; " +
                    "                        rdf:object account:" + w14.getLoanId() + " ; " +
                    "                        ex:provenance [" +
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
            GraphDbReification.logger.info(w15.toString());

            //Add Deposit Between Loan And Account

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX loan: <http://example.org/Loan/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject loan:" + w15.getLoanId() + " ; " +
                    "                        rdf:predicate ex:deposit ; " +
                    "                        rdf:object account:" + w15.getAccountId() + " ; " +
                    "                        ex:provenance  [" +
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
            GraphDbReification.logger.info(w16.toString());

            //Account signed in with Medium
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w16.getAccountId());
            queryParams.put("mediumId", w16.getMediumId());
            queryParams.put("time", DATE_FORMAT.format(w16.getTime()));

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX account: <http://example.org/Account/> " +
                    "PREFIX medium: <http://example.org/Medium/> " +
                    "            INSERT DATA{ " +
                    "                    [] rdf:subject account:" + w16.getMediumId() + " ; " +
                    "                        rdf:predicate ex:signIn ; " +
                    "                        rdf:object account:" + w16.getAccountId() + " ; " +
                    "                        ex:provenance  [" +
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
            GraphDbReification.logger.info(w17.toString());

            //Remove an Account
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w17.getAccountId());

            String queryString = "PREFIX ex: <http://example.org/>\n" +
                    "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" +
                    "PREFIX account: <http://example.org/Account/>\n" +
                    "\n" +
                    "# Löschen des Accounts und aller zugehörigen Tripel\n" +
                    "DELETE {\n" +
                    "  # Löscht alle Tripel, in denen der Account als Subjekt oder Objekt beteiligt ist\n" +
                    "  ?s ?p ?o .\n" +
                    "  ?s ?p ?account .\n" +
                    "  ?account ?p2 ?o2 .\n" +
                    "  # Löscht alle Tripel, die `Loan`-Objekte betreffen, die mit dem Account verbunden sind\n" +
                    "  ?loan ex:deposit ?account .\n" +
                    "  ?account ex:repay ?loan .\n" +
                    "  # Löscht alle Provenance-Informationen, die den Account betreffen\n" +
                    "  ?occ1 rdf:subject ?account ; rdf:predicate ?a ; rdf:object ?b ; ex:provenance ?occurrence1 .\n" +
                    "  ?occ2 rdf:subject ?c ; rdf:predicate ?d ; rdf:object ?account ; ex:provenance ?occurrence2 .\n" +
                    "}\n" +
                    "WHERE {\n" +
                    "  # Account-ID binden\n" +
                    "  BIND(account:" + w17.getAccountId() + " AS ?account)\n" +
                    "\n" +
                    "  # Löschen der Tripel, in denen der Account als Subjekt auftritt\n" +
                    "  { ?account ?p ?o . }\n" +
                    "  UNION\n" +
                    "  # Löschen der Tripel, in denen der Account als Objekt auftritt\n" +
                    "  { ?s ?p ?account . }\n" +
                    "  UNION\n" +
                    "  # Löschen der Kanten wie `own`, `transfer`, `withdraw`, `repay`, `deposit`, `signIn`\n" +
                    "  { ?s ?p ?account . ?account ?p2 ?o2 . }\n" +
                    "  UNION\n" +
                    "  # Löschen von Loan-Objekten, die mit dem Account verbunden sind\n" +
                    "  { ?account ex:repay ?loan . ?loan ex:deposit ?account . }\n" +
                    "  UNION\n" +
                    "  # Löschen von Provenance-Tripeln (Ereignisse, die den Account betreffen)\n" +
                    "  { ?occ1 rdf:subject ?account ; rdf:predicate ?a ; rdf:object ?b ; ex:provenance ?occurrence1 . }\n" +
                    "  UNION\n" +
                    "  { ?occ2 rdf:subject ?c ; rdf:predicate ?d ; rdf:object ?account ; ex:provenance ?occurrence2 . }\n" +
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
            GraphDbReification.logger.info(w18.toString());

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
            GraphDbReification.logger.info(w19.toString());

            //Block a Person of high risk
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w19.getPersonId());

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "PREFIX person: <http://example.org/Person/> " +
                    "            INSERT DATA{ " +
                    "                     person:" + w19.getPersonId() + " ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
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
            GraphDbReification.logger.info(rw1.toString());
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
                        " [] rdf:subject account:" + rw1.getSrcId() + " ;" +
                        "    rdf:predicate ex:transfer ;" +
                        "    rdf:object account:" + rw1.getDstId() + " ;" +
                        "    ex:provenance [" +
                        "                     a ex:transfer ;" +
                        "                     ex:amount \"" + rw1.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                        "                     ex:createTime \""+ DATE_FORMAT.format(rw1.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] ."+
                        "            }";


                RepositoryConnection connection = client.startTransaction(write12String);

                if(!connection.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                    return;
                }
                String complexRead4String = "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                        "PREFIX ex: <http://example.org/>\n" +
                        "PREFIX account: <http://example.org/Account/>\n" +
                        "\n" +
                        "SELECT \n" +
                        " ?otherId" +
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
                        "    ?blankNode1 rdf:subject ?src .\n" +
                        "    ?blankNode1 rdf:predicate ex:transfer .\n" +
                        "    ?blankNode1 rdf:object ?dst .\n" +
                        "    ?blankNode1 ex:provenance ?occurrence1 .\n" +
                        "    ?occurrence1 ex:createTime ?createTime1 .\n" +
                        "    ?occurrence1 ex:amount ?amount1 ." +
                        "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(rw1.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(rw1.getEndTime())+"\"))\n" +
                        "    \n" +
                        "    # Step 2: Find all other accounts that received money from dst and transferred money to src\n" +
                        "    ?blankNode2 rdf:subject ?dst .\n" +
                        "    ?blankNode2 rdf:predicate ex:transfer .\n" +
                        "    ?blankNode2 rdf:object ?other .\n" +
                        "    ?blankNode2 ex:provenance ?occurrence2 .\n" +
                        "    ?occurrence2 ex:createTime ?createTime2 .\n" +
                        "    ?occurrence2 ex:amount ?amount2 ." +
                        "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(rw1.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(rw1.getEndTime())+"\"))\n" +
                        "    \n" +
                        "    ?blankNode3 rdf:subject ?other .\n" +
                        "    ?blankNode3 rdf:predicate ex:transfer .\n" +
                        "    ?blankNode3 rdf:object ?src .\n" +
                        "    ?blankNode3 ex:provenance ?occurrence3 .\n" +
                        "    ?occurrence3 ex:createTime ?createTime3 .\n" +
                        "    ?occurrence3 ex:amount ?amount3 ." +
                        "    FILTER(?createTime3 > xsd:dateTime(\""+DATE_FORMAT.format(rw1.getStartTime())+"\") && ?createTime3 < xsd:dateTime(\""+DATE_FORMAT.format(rw1.getEndTime())+"\"))\n" +
                        "    \n" +
                        "    BIND(STRAFTER(STR(?other), \"http://example.org/Account/\") AS ?otherId)\n" +
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
                        " PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                     account:"+rw1.getSrcId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";
                String write18StringDst = "PREFIX ex: <http://example.org/> " +
                        " PREFIX account: <http://example.org/Account/> " +
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
                        " [] rdf:subject account:" + rw2.getSrcId() + " ;" +
                        "    rdf:predicate ex:transfer ;" +
                        "    rdf:object account:" + rw2.getDstId() + " ;" +
                        "    ex:provenance [" +
                        "                     a ex:transfer ;" +
                        "                     ex:amount \"" + rw2.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                        "                     ex:createTime \""+ DATE_FORMAT.format(rw2.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] ."+
                        "            }";


                RepositoryConnection connection = client.startTransaction(write12String);

                if(!connection.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                    return;
                }
                List<Float> ratios = new ArrayList<>();

                for(Long id: new Long[]{rw2.getSrcId(), rw2.getDstId()}) {


                    String complexRead7String = "PREFIX ex: <http://example.org/>\n" +
                            "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" +
                            "PREFIX account: <http://example.org/Account/>\n" +
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
                            "  ?blankNode1 rdf:subject ?src ;\n" +
                            "              rdf:predicate ex:transfer ;\n" +
                            "              rdf:object ?mid ;\n" +
                            "              ex:provenance ?edge1Occurrence .\n" +
                            "  ?edge1Occurrence ex:amount ?edge1Amount ;\n" +
                            "                   ex:createTime ?createTime1 . " +
                            "    FILTER(?createTime1 > xsd:dateTime(\""+DATE_FORMAT.format(rw2.getStartTime())+"\") && ?createTime1 < xsd:dateTime(\""+DATE_FORMAT.format(rw2.getEndTime())+"\"))\n" +
                            "    FILTER(?edge1Amount > "+rw2.getAmountThreshold()+")\n" +

                            " ?blankNode2 rdf:subject ?mid .\n" +
                            "  ?blankNode2 rdf:predicate ex:transfer .\n" +
                            "  ?blankNode2 rdf:object ?dst .\n" +
                            "  ?blankNode2 ex:provenance ?edge2Occurrence .\n" +
                            "  ?edge2Occurrence ex:amount ?edge2Amount .\n" +
                            "  ?edge2Occurrence ex:createTime ?createTime2 ." +
                            "    FILTER(?createTime2 > xsd:dateTime(\""+DATE_FORMAT.format(rw2.getStartTime())+"\") && ?createTime2 < xsd:dateTime(\""+DATE_FORMAT.format(rw2.getEndTime())+"\"))\n" +
                            "    FILTER(?edge2Amount > "+rw2.getAmountThreshold()+")\n" +
                            "\n" +
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
                        " PREFIX account: <http://example.org/Account/> " +
                        "            INSERT DATA{ " +
                        "                     account:"+rw2.getSrcId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";
                String write18StringDst = "PREFIX ex: <http://example.org/> " +
                        " PREFIX account: <http://example.org/Account/> " +
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
            GraphDbReification.logger.info(rw3.toString());

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
                        "PREFIX person: <http://example.org/Person/> " +
                        "            INSERT DATA{ " +
                        " [] rdf:subject person:" + rw3.getSrcId() + " ;" +
                        "    rdf:predicate ex:guarantee ;" +
                        "    rdf:object person:" + rw3.getDstId() + " ;" +
                        "    ex:provenance [" +
                        "                     a ex:guarantee ;" +
                        "                     ex:createTime \""+ DATE_FORMAT.format(rw3.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ] ."+
                        "            }";

                RepositoryConnection connection = client.startTransaction(write10String);

                if(!connection.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                    return;
                }
                //HIER CR11
                String complexRead11String = "";

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
                        "PREFIX person: <http://example.org/Person> " +
                        "            INSERT DATA{ " +
                        "                     person:"+rw3.getSrcId()+" ex:isBlocked \"true\"^^<http://www.w3.org/2001/XMLSchema#boolean> ." +
                        "            }";
                String write19StringDst = "PREFIX ex: <http://example.org/> " +
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
