package org.ldbcouncil.finbench.impls.dummy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ldbcouncil.finbench.driver.*;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;
import org.neo4j.driver.Result;
import org.neo4j.driver.Transaction;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class MemgraphDb extends Db {
    static Logger logger = LogManager.getLogger("DummyDb");

    private CypherDbConnectionState connectionState = null;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {

        connectionState = new CypherDbConnectionState(map);
        logger.info("MemgraphDb initialized");

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
        logger.info("MemgraphDb closed");
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            MemgraphDb.logger.info(cr1);

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr1.getId() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr1.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr1.getEndTime()));

            String queryString = "MATCH p=(account:Account {accountId: $id})-[edge1:transfer*1..3]->(other:Account), "
                    + "(other)<-[edge2:signIn]-(medium:Medium {isBlocked: true}) "
                    + "WITH p, extract(e IN relationships(p) | e.createTime) AS ts, other, medium "
                    + "WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x " +
                    "ELSE localDateTime(\"9999-12-30T23:59:59\") end) <> localDateTime(\"9999-12-30T23:59:59\") "
                    + "AND all(e IN edge1 WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) "
                    + "AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) "
                    + "RETURN DISTINCT " +
                    "other.accountId AS otherId, " +
                    "size(p) AS accountDistance, " +
                    "medium.mediumId AS mediumId, " +
                    "medium.mediumType AS mediumType " +
                    "ORDER BY accountDistance ASC, " +
                    "size(otherId) ASC, " + //WORK AROUND SORT BECAUSE ID IS A STRING
                    "otherId ASC, " +
                    "size(mediumId) ASC, " +
                    "mediumId ASC ";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead1Result> complexRead1Results = null;
            try {
                complexRead1Results = cr1.deserializeResult(result);
                resultReporter.report(complexRead1Results.size(), complexRead1Results, cr1);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr1);
            }


        }
    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            MemgraphDb.logger.info(cr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr2.getId() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr2.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr2.getEndTime()));

            String queryString = "MATCH " +
                    "    (person:Person {personId: $id})-[edge1:own]->(accounts:Account), " +
                    "    p=(accounts)<-[edge2:transfer*1..3]-(other:Account), " +
                    "    (other)<-[edge3:deposit]-(loan:Loan) " +
                    "WITH p, " +
                    "     extract(e IN relationships(p) | e.createTime) AS ts, " +
                    "     other, " +
                    "     loan, " +
                    "     other IS NOT NULL AS otherExists " +
                    "WHERE " +
                    "    otherExists " +
                    "    AND reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x " +
                    "ELSE localDateTime(\"9999-12-30T23:59:59\") END) <> localDateTime(\"9999-12-30T23:59:59\") " +
                    "    AND all(e IN edge2 WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "    AND localDateTime($start_time) < edge3.createTime < localDateTime($end_time) " +
                    "RETURN " +
                    "    other.accountId AS otherId, " +
                    "    (round(1000 * (REDUCE(total = 0, loanA IN COLLECT(DISTINCT loan) | total + loanA.loanAmount)))/1000) AS sumLoanAmount, " +
                    "    (round(1000 * (REDUCE(total = 0, loanA IN COLLECT(DISTINCT loan) | total + loanA.balance)))/1000) AS sumLoanBalance " +
                    "ORDER BY " +
                    "    sumLoanAmount DESC, " +
                    "    size(otherId) ASC, " +     //WORK AROUND SORT BECAUSE ID IS A STRING
                    "    otherId ASC";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead2Result> complexRead2Results = null;
            try {
                complexRead2Results = cr2.deserializeResult(result);
                resultReporter.report(complexRead2Results.size(), complexRead2Results, cr2);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + cr2);
                resultReporter.report(0, new ArrayList<>(), cr2);
            }
        }
    }

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            MemgraphDb.logger.info(cr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr3.getId1() + "");
            queryParams.put("id2", cr3.getId2() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr3.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr3.getEndTime()));

            //Breadth-first search (BFS)
            //BFS is ideal for finding the shortest path between two nodes in an unweighted graph or between the
            // start node and any other node in the graph. Since it traverses all nodes at a given depth before moving
            // to the next level, it ensures that the shortest path is found.
            String queryString = "MATCH path1=(src:Account {accountId: $id1})-[edge:transfer *BFS (r, n | localDateTime($start_time) < r.createTime < localDateTime($end_time)) ]->" +
                    "(dst:Account {accountId: $id2})" +
                    "RETURN size(path1) AS shortestPathLength";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead3Result> complexRead3Results = null;
            try {
                complexRead3Results = cr3.deserializeResult(result);
                resultReporter.report(complexRead3Results.size(), complexRead3Results, cr3);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + cr3);
                resultReporter.report(0, new ArrayList<>(), cr3);
            }
        }
    }

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            MemgraphDb.logger.info(cr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr4.getId1() + "");
            queryParams.put("id2", cr4.getId2() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr4.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr4.getEndTime()));

            String queryString = "MATCH \n" +
                    "    (src:Account {accountId: $id1})-[edge1:transfer]->(dst:Account {accountId: $id2}), \n" +
                    "    (dst)-[edge3:transfer]->(other:Account)-[edge2:transfer]->(src) \n" +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                    "AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) " +
                    "AND localDateTime($start_time) < edge3.createTime < localDateTime($end_time) " +
                    "WITH \n" +
                    "    other.accountId as otherId,\n" +
                    "    count(DISTINCT edge3) AS numEdge3, \n" +
                    "    round(1000 * sum(DISTINCT edge3.amount)) / 1000 AS sumEdge3Amount,\n" +
                    "    round(1000 * max(edge3.amount)) / 1000 AS maxEdge3Amount,\n" +
                    "    count(DISTINCT edge2) AS numEdge2, \n" +
                    "    round(1000 * sum(DISTINCT edge2.amount)) / 1000 AS sumEdge2Amount,\n" +
                    "    round(1000 * max(edge2.amount)) / 1000 AS maxEdge2Amount\n" +
                    "// Optional match for outgoing transfers from dst to other\n" +
                    "RETURN otherId, numEdge2, sumEdge2Amount,maxEdge2Amount, \n" +
                    "     numEdge3, sumEdge3Amount, maxEdge3Amount\n" +
                    "ORDER BY sumEdge2Amount DESC,sumEdge3Amount DESC, otherId ASC;\n";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead4Result> complexRead4Results = null;
            try {
                complexRead4Results = cr4.deserializeResult(result);
                resultReporter.report(complexRead4Results.size(), complexRead4Results, cr4);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + cr4);
                resultReporter.report(0, new ArrayList<>(), cr4);
            }
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            MemgraphDb.logger.info(cr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr5.getId() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr5.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr5.getEndTime()));
            queryParams.put("last_time", "9999-12-30T23:59:59");

            String queryString = "MATCH " +
                    "(person:Person {personId: $id})-[edge1:own]->(src:Account), " +
                    "p=(src)-[edge2:transfer*1..3]->(dst:Account) " +
                    "WITH p, extract(e IN relationships(p) | e.createTime) AS ts, " +
                    "nodes(p) AS nodeList " +
                    "WHERE "+
                    "reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x " +
                    "ELSE localDateTime($last_time) end) <> localDateTime($last_time) " +
                    "AND all(e IN edge2 WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "RETURN DISTINCT extract(node in nodeList | node.accountId) AS path " +
                    "ORDER BY size(path) DESC";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead5Result> complexRead5Results = null;
            try {
                complexRead5Results = cr5.deserializeResult(result);
                resultReporter.report(complexRead5Results.size(), complexRead5Results, cr5);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + cr5);
                resultReporter.report(0, new ArrayList<>(), cr5);
            }
        }
    }

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            MemgraphDb.logger.info(cr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr6.getId() + "");
            queryParams.put("threshold1", cr6.getThreshold1());
            queryParams.put("threshold2", cr6.getThreshold2());
            queryParams.put("start_time", DATE_FORMAT.format(cr6.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr6.getEndTime()));

            String queryString = "MATCH (src1:Account)-[edge1:transfer]->(mid:Account)-[edge2:withdraw]->(dstCard:Account {accountId: $id})\n" +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) \n" +
                    "  AND edge1.amount > $threshold1 \n" +
                    "  AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) \n" +
                    "  AND edge2.amount > $threshold2 \n" +
                    "WITH mid, count(edge1) AS transferCount, " +
                    "(round(1000 * REDUCE(total = 0, edge IN COLLECT(DISTINCT edge1) | total + edge.amount))/1000) AS sumEdge1Amount, " +           //ADDED ROUND
                    "(round(1000 * (REDUCE(total = 0, edge IN COLLECT(DISTINCT edge2) | total + edge.amount)))/1000) AS sumEdge2Amount " +
                    "WHERE transferCount > 3 " +
                    "RETURN " +
                    "  mid.accountId AS midId, " +
                    "  sumEdge1Amount, " +
                    "  sumEdge2Amount " +
                    "ORDER BY sumEdge2Amount DESC, midId ASC";
            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead6Result> complexRead6Results = null;
            try {
                complexRead6Results = cr6.deserializeResult(result);
                resultReporter.report(complexRead6Results.size(), complexRead6Results, cr6);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + cr6);
                resultReporter.report(0, new ArrayList<>(), cr6);
            }
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr7.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr7.getId() + "");
            queryParams.put("threshold", cr7.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr7.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr7.getEndTime()));

            String queryString = "MATCH (src:Account)-[edge1:transfer]->(mid:Account {accountId: $id})-[edge2:transfer]->(dst:Account)\n" +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) \n" +
                    "                    AND edge1.amount > $threshold \n" +
                    "                    AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) \n" +
                    "                    AND edge2.amount > $threshold \n" +
                    "WITH REDUCE(total = 0, account IN COLLECT(DISTINCT edge1) | total + account.amount) AS sumEdge1Amount, " +
                    "       COUNT(DISTINCT src) AS numSrc\n" +
                    "OPTIONAL MATCH (mid:Account {accountId: $id})-[edge2:transfer]->(dst:Account)\n" +
                    "WHERE localDateTime($start_time) < edge2.createTime < localDateTime($end_time) \n" +
                    "                    AND edge2.amount > $threshold \n" +
                    "WITH sumEdge1Amount, numSrc," +
                    "REDUCE(total = 0, account IN COLLECT(DISTINCT edge2) | total + account.amount) AS sumEdge2Amount,\n"+
                    "       COUNT(DISTINCT dst) AS numDst\n" +
                    "RETURN numSrc, numDst," +
                    "       CASE " +
                    "       WHEN sumEdge2Amount > 0 THEN ROUND(1000 * sumEdge1Amount / sumEdge2Amount) / 1000 " +
                    "       ELSE -1 " +
                    "       END AS inOutRatio " +
                    "ORDER BY numSrc DESC, numDst DESC, inOutRatio DESC";
            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead7Result> complexRead7Results = null;
            try {
                complexRead7Results = cr7.deserializeResult(result);
                resultReporter.report(complexRead7Results.size(), complexRead7Results, cr7);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + cr7);
                resultReporter.report(0, new ArrayList<>(), cr7);
            }
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr8.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr8.getId() + "");
            queryParams.put("threshold", cr8.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr8.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr8.getEndTime()));

            String queryString = "MATCH " +
                    "(loan:Loan {loanId: $id})-[edge1:deposit]->(src:Account), " +
                    "p=(src)-[edge234:transfer|withdraw*1..3]->(dst:Account) " +
                    "WITH loan, p, dst, extract(e IN relationships(p) | e.amount) AS amts " +
                    "WHERE " +
                    "localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                    "AND all(e IN edge234 WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "AND reduce(curr = head(amts), x IN tail(amts) | CASE WHEN (curr <> -1) " +
                    "AND (x > curr*$threshold) THEN x ELSE -1 end) <> -1 " +
                    "WITH loan, size(p)+1 AS minDistanceFromLoan, dst, REDUCE(total = 0, amount IN COLLECT(DISTINCT last(amts)) | total + amount) AS inflow " +
                    "RETURN dst.accountId AS dstId, round(1000 * inflow/loan.loanAmount) / 1000 AS ratio, minDistanceFromLoan " +
                    "ORDER BY minDistanceFromLoan DESC, ratio DESC, size(dstId) ASC, dstId ASC"; //WORK AROUND SORT BECAUSE ID IS A STRING


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead8Result> complexRead8Results = null;
            try {
                complexRead8Results = cr8.deserializeResult(result);
                resultReporter.report(complexRead8Results.size(), complexRead8Results, cr8);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + cr8);
                resultReporter.report(0, new ArrayList<>(), cr8);
            }
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr9.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr9.getId() + "");
            queryParams.put("threshold", cr9.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr9.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr9.getEndTime()));

            String queryString = "OPTIONAL MATCH \n" +
                    "    (loan:Loan)-[edge1:deposit]->(mid:Account {accountId: $id})\n" +
                    "WHERE \n" +
                    "    edge1.amount > $threshold AND localDateTime($start_time) < edge1.createTime < localDateTime($end_time) \n" +
                    "WITH \n" +
                    "    REDUCE(total = 0, edge IN COLLECT(DISTINCT edge1) | total + edge.amount) AS sumEdge1Repay " +
                    "\n" +
                    "OPTIONAL MATCH \n" +
                    "    (mid:Account {accountId: $id})-[edge2:repay]->(loan2:Loan)\n" +
                    "WHERE \n" +
                    "    edge2.amount > $threshold AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time)\n" +
                    "WITH \n" +
                    "    sumEdge1Repay, REDUCE(total = 0, edge IN COLLECT(DISTINCT edge2) | total + edge.amount) AS sumEdge2Repay " +
                    "OPTIONAL MATCH \n" +
                    "    (up:Account)-[edge3:transfer]->(mid:Account {accountId: $id})\n" +
                    "WHERE \n" +
                    "    edge3.amount > $threshold AND localDateTime($start_time) < edge3.createTime < localDateTime($end_time)\n" +
                    "WITH \n" +
                    "    sumEdge1Repay, sumEdge2Repay, \n" +
                    "    REDUCE(total = 0, edge IN COLLECT(DISTINCT edge3) | total + edge.amount) AS sumEdge3Transfer \n" +
                    "\n" +
                    "OPTIONAL MATCH \n" +
                    "    (mid:Account {accountId: $id})-[edge4:transfer]->(down:Account)\n" +
                    "WHERE \n" +
                    "    edge4.amount > $threshold AND localDateTime($start_time) < edge4.createTime < localDateTime($end_time)\n" +
                    "WITH \n" +
                    "    sumEdge1Repay, sumEdge2Repay, sumEdge3Transfer,\n" +
                    "    REDUCE(total = 0, edge IN COLLECT(DISTINCT edge4) | total + edge.amount) AS sumEdge4Transfer\n" +
                    "\n" +
                    "RETURN\n" +
                    "    CASE \n" +
                    "        WHEN sumEdge2Repay > 0 THEN round(1000 * sumEdge1Repay / sumEdge2Repay) / 1000 \n" +
                    "        ELSE -1 \n" +
                    "    END AS ratioRepay, \n" +
                    "    CASE \n" +
                    "        WHEN sumEdge4Transfer > 0 THEN round(1000 * sumEdge1Repay / sumEdge4Transfer) / 1000 \n" +
                    "        ELSE -1 \n" +
                    "    END AS ratioDeposit, \n" +
                    "    CASE \n" +
                    "        WHEN sumEdge4Transfer > 0 THEN round(1000 * sumEdge3Transfer / sumEdge4Transfer) / 1000 \n" +
                    "        ELSE -1 \n" +
                    "    END AS ratioTransfer;\n";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead9Result> complexRead9Results = null;
            try {
                complexRead9Results = cr9.deserializeResult(result);
                resultReporter.report(complexRead9Results.size(), complexRead9Results, cr9);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + cr9);
                resultReporter.report(0, new ArrayList<>(), cr9);
            }
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            MemgraphDb.logger.info(cr10.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr10.getPid1() + "");
            queryParams.put("id2", cr10.getPid2() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr10.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr10.getEndTime()));

            String queryString = "MATCH " +
                    "(p1:Person {personId: $id1})-[edge1:invest]->(m1:Company), " +
                    "(p2:Person {personId: $id2})-[edge2:invest]->(m2:Company) " +
                    "WITH collect(m1) as m1List, collect(m2) as m2List " +
                    "WITH " +
                    "toFloat(size(collections.intersection(m1List, m2List))) AS intersection, " +
                    "size(collections.union(m1List, m2List)) AS union " +
                    "RETURN round(1000 * (intersection)/union) / 1000 AS jaccardSimilarity";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead10Result> complexRead10Results = null;
            try {
                complexRead10Results = cr10.deserializeResult(result);
                resultReporter.report(complexRead10Results.size(), complexRead10Results, cr10);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + cr10);
                resultReporter.report(0, new ArrayList<>(), cr10);
            }
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr11.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr11.getId() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr11.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr11.getEndTime()));

            String queryString = "MATCH path=(p1:Person {personId: $id})-[:guarantee*]->(pX:Person) " +
                    "WHERE all(e IN relationships(path) WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "UNWIND nodes(path)[1..] AS person " +
                    "MATCH (person)-[:apply]->(loan:Loan) " +
                    "RETURN " +
                    "(round(1000 * REDUCE(total = 0, loanA IN COLLECT(DISTINCT loan) | total + loanA.loanAmount))/1000) AS sumLoanAmount, " +
                    "count(DISTINCT loan) AS numLoans";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead11Result> complexRead11Results = null;
            try {
                complexRead11Results = cr11.deserializeResult(result);
                resultReporter.report(complexRead11Results.size(), complexRead11Results, cr11);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + cr11);
                resultReporter.report(0, new ArrayList<>(), cr11);
            }
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, CypherDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr12.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr12.getId() + "");
            queryParams.put("start_time", DATE_FORMAT.format(cr12.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr12.getEndTime()));

            String queryString = "MATCH (person:Person {personId: $id}) " +
                    "-[edge1:own]->(pAcc:Account) " +
                    "-[edge2:transfer]->(compAcc:Account) " +
                    "<-[edge3:own]-(company:Company) " +
                    "WHERE localDateTime($start_time) < edge2.createTime < localDateTime($end_time) " +
                    "RETURN " +
                    "compAcc.accountId AS compAccountId, " +
                    "(round(1000 * REDUCE(total = 0, edge IN COLLECT(DISTINCT edge2) | total + edge.amount))/1000) AS sumEdge2Amount " +
                    "ORDER BY sumEdge2Amount DESC, size(compAccountId) ASC, compAccountId ASC";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead12Result> complexRead12Results = null;
            try {
                complexRead12Results = cr12.deserializeResult(result);
                resultReporter.report(complexRead12Results.size(), complexRead12Results, cr12);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + cr12);
                resultReporter.report(0, new ArrayList<>(), cr12);
            }
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, CypherDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr1.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr1.getId() + "");

            String queryString = "MATCH (account:Account {accountId: $id}) " +
                    "RETURN account.createTime AS createTime, account.isBlocked AS isBlocked, account.accountType AS type";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead1Result> simpleRead1Results = null;
            try {
                simpleRead1Results = sr1.deserializeResult(result);
                resultReporter.report(simpleRead1Results.size(), simpleRead1Results, sr1);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + sr1);
                resultReporter.report(0, new ArrayList<>(), sr1);
            }

        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, CypherDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr2.getId() + "");
            queryParams.put("start_time", DATE_FORMAT.format(sr2.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr2.getEndTime()));

            String queryString = "MATCH (src:Account {accountId: $id}) \n" +
                    "OPTIONAL MATCH (src)-[edge1:transfer]->(dst1:Account) \n" +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) \n" +
                    "OPTIONAL MATCH (src)<-[edge2:transfer]-(dst2:Account) \n" +
                    "WHERE localDateTime($start_time) < edge2.createTime < localDateTime($end_time) \n" +
                    "\n" +
                    "                    WITH COLLECT(DISTINCT edge1) AS edges1, \n" +
                    "                        COLLECT(DISTINCT edge2) AS edges2,\n" +
                    "                        max(edge1.amount) AS maxEdge1AmountRaw, \n" +
                    "                        max(edge2.amount) AS maxEdge2AmountRaw\n" +
                    "                    RETURN \n" +
                    "                        (round(1000 * REDUCE(total = 0, edge IN edges1 | total + edge.amount))/1000) AS sumEdge1Amount, \n" +
                    "                        CASE \n" +
                    "                            WHEN maxEdge1AmountRaw IS NOT NULL THEN round(1000 * maxEdge1AmountRaw)/1000 \n" +
                    "                            ELSE -1 \n" +
                    "                        END AS maxEdge1Amount, \n" +
                    "                        size(edges1) AS numEdge1, \n" +
                    "                        (round(1000 * REDUCE(total = 0, edge IN edges2 | total + edge.amount))/1000) AS sumEdge2Amount, \n" +
                    "                        CASE \n" +
                    "                            WHEN maxEdge2AmountRaw IS NOT NULL THEN round(1000 * maxEdge2AmountRaw)/1000 \n" +
                    "                            ELSE -1 \n" +
                    "                        END AS maxEdge2Amount, \n" +
                    "                        size(edges2) AS numEdge2";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead2Result> simpleRead2Results = null;
            try {
                simpleRead2Results = sr2.deserializeResult(result);
                resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + sr2);
                resultReporter.report(0, new ArrayList<>(), sr2);
            }

        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, CypherDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr3.getId() + "");
            queryParams.put("threshold", sr3.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr3.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr3.getEndTime()));

            String queryString = "MATCH (src:Account)-[edge2:transfer]->(dst:Account {accountId: $id}) " +
                    "OPTIONAL MATCH (blockedSrc:Account {isBlocked: true})-[edge1:transfer]->(dst) " +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                    "AND edge1.amount > $threshold " +
                    "WITH count(DISTINCT edge1) AS blockedTransfers, count(DISTINCT edge2) AS totalTransfers " +
                    "RETURN CASE " +
                    "    WHEN totalTransfers > 0 THEN " +
                    "        round(1000 * blockedTransfers / totalTransfers) / 1000 " +
                    "    ELSE " +
                    "        -1 " +
                    "END AS blockRatio ";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead3Result> simpleRead3Results = null;
            try {
                simpleRead3Results = sr3.deserializeResult(result);
                resultReporter.report(simpleRead3Results.size(), simpleRead3Results, sr3);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + sr3);
                resultReporter.report(0, new ArrayList<>(), sr3);
            }
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, CypherDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr4.getId() + "");
            queryParams.put("threshold", sr4.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr4.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr4.getEndTime()));

            String queryString = "MATCH (src:Account {accountId: $id})-[edge:transfer]->(dst:Account) " +
                    "WHERE localDateTime($start_time) < edge.createTime < localDateTime($end_time) " +
                    "AND edge.amount > $threshold " +
                    "RETURN " +
                    "dst.accountId AS dstId, " +
                    "count(edge) AS numEdges, " +
                    "(round(1000 * sum(edge.amount))/1000) AS sumAmount " +
                    "ORDER BY sumAmount DESC, size(dstId) ASC, dstId ASC";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead4Result> simpleRead4Results = null;
            try {
                simpleRead4Results = sr4.deserializeResult(result);
                resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + sr4);
                resultReporter.report(0, new ArrayList<>(), sr4);
            }
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, CypherDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr5.getId() + "");
            queryParams.put("threshold", sr5.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr5.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr5.getEndTime()));

            String queryString = "MATCH (dst:Account {accountId: $id})<-[edge:transfer]-(src:Account) " +
                    "WHERE localDateTime($start_time) < edge.createTime < localDateTime($end_time) " +
                    "  AND edge.amount > $threshold " +
                    "RETURN " +
                    "src.accountId AS srcId, " +
                    "count(edge) AS numEdges, " +
                    "(round(1000 * sum(edge.amount))/1000) AS sumAmount " +
                    "ORDER BY sumAmount DESC, size(srcId) ASC, srcId ASC";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead5Result> simpleRead5Results = null;
            try {
                simpleRead5Results = sr5.deserializeResult(result);
                resultReporter.report(simpleRead5Results.size(), simpleRead5Results, sr5);
            } catch (IOException e) {
                Neo4jDb.logger.warn(e.getMessage() + "\n" + sr5);
                resultReporter.report(0, new ArrayList<>(), sr5);
            }
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, CypherDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr6.getId() + "");
            queryParams.put("start_time", DATE_FORMAT.format(sr6.getStartTime().getTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr6.getEndTime().getTime()));

            String queryString = "MATCH (src:Account {accountId: $id})<-[e1:transfer]-(mid:Account)-[e2:transfer]->" +
                    "(dst:Account {isBlocked: true}) " +
                    "WHERE src.accountId <> dst.accountId " +
                    "  AND localDateTime($start_time) < e1.createTime < localDateTime($end_time) " +
                    "  AND localDateTime($start_time) < e2.createTime < localDateTime($end_time) " +
                    "RETURN DISTINCT dst.accountId AS dstId " +
                    "ORDER BY size(dstId) ASC, dstId ASC";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead6Result> simpleRead6Results = null;
            try {
                simpleRead6Results = sr6.deserializeResult(result);
                resultReporter.report(simpleRead6Results.size(), simpleRead6Results, sr6);
            } catch (IOException e) {
                MemgraphDb.logger.warn(e.getMessage() + "\n" + sr6);
                resultReporter.report(0, new ArrayList<>(), sr6);
            }
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w1.toString());

            //Add a person Node
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId", w1.getPersonId() + "");
            queryParams.put("personName", w1.getPersonName());
            queryParams.put("createTime", DATE_FORMAT.format(new Date()));
            queryParams.put("isBlocked", w1.getIsBlocked());

            /*
            String queryString = "MERGE (p:Person {personId: $personId})" +
                    "ON CREATE SET p.personName = $personName, " +
                    "p.isBlocked = $isBlocked, p.createTime = localDateTime($createTime)";


             */
            String queryString = "CREATE (:Person {personId: $personId, personName: $personName, " +
                    "                    isBlocked: $isBlocked, createTime: localDateTime($createTime)})";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w1);
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w2.toString());

            //Add a Company Node
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("companyId", w2.getCompanyId() + "");
            queryParams.put("companyName", w2.getCompanyName());
            queryParams.put("createTime", DATE_FORMAT.format(new Date()));
            queryParams.put("isBlocked", w2.getIsBlocked());

            /*
            String queryString = "MERGE (c:Company {companyId: $companyId})" +
                    "ON CREATE SET c.companyName = $companyName, " +
                    "c.createTime = localDateTime($createTime), c.isBlocked = $isBlocked";


             */
            String queryString = "CREATE (:Company {companyId: $companyId, companyName: $companyName, " +
                    "                    isBlocked: $isBlocked, createTime: localDateTime($createTime)})";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w2);
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w3.toString());

            //Add a Medium Node
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("mediumId", w3.getMediumId() + "");
            queryParams.put("mediumType", w3.getMediumType());
            queryParams.put("createTime", DATE_FORMAT.format(new Date()));

            /*
            String queryString = "MERGE (m:Medium {mediumId: $mediumId})" +
                    "ON CREATE SET m.mediumType = $mediumType, m.createTime = localDateTime($createTime)";


             */
            String queryString = "CREATE (:Medium {mediumId: $mediumId, mediumType: $mediumType, " +
                    "                    createTime: localDateTime($createTime)})";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w3);
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w4.toString());

            //Add an Account Node owned by Person
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId", w4.getPersonId()+"");
            queryParams.put("accountId", w4.getAccountId()+"");
            queryParams.put("time", DATE_FORMAT.format(w4.getTime()));
            queryParams.put("accountBlocked", w4.getAccountBlocked());
            queryParams.put("accountType", w4.getAccountType());

            String queryString = "MATCH (p:Person {personId: $personId}) " +
                    "CREATE (p)-[:own {createTime: $time}]->" +
                    "(:Account {accountId: $accountId, " +
                    "createTime: localDateTime($time), isBlocked: $accountBlocked, accountType: $accountType})";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w4);
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w5.toString());

            //Add an Account Node owned by Company
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("companyId", w5.getCompanyId()+"");
            queryParams.put("accountId", w5.getAccountId()+"");
            queryParams.put("time", DATE_FORMAT.format(w5.getTime()));
            queryParams.put("accountBlocked", w5.getAccountBlocked());
            queryParams.put("accountType", w5.getAccountType());

            String queryString = "MATCH (c:Company {companyId: $companyId}) " +
                    "CREATE (c)-[:own {createTime: $time}]->" +
                    "(:Account {accountId: $accountId, " +
                    "createTime: localDateTime($time), isBlocked: $accountBlocked, type: $accountType})";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w5);
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w6.toString());

            //Add Loan applied by Person
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId", w6.getPersonId()+"");
            queryParams.put("loanId", w6.getLoanId()+"");
            queryParams.put("time", DATE_FORMAT.format(w6.getTime()));
            queryParams.put("balance", w6.getBalance());
            queryParams.put("loanAmount", w6.getLoanAmount());

            /*
            String queryString = "MATCH (p:Person {personId: $personId}) " +
                    "MERGE (l:Loan {loanId: $loanId}) " +
                    "SET l.loanAmount = $loanAmount, " +
                    "l.balance = $balance, l.createTime = localDateTime($time) " +
                    "CREATE (l)<-[:apply {createTime: localDateTime($time)}]-(p)";

             */
            String queryString = "MATCH (p:Person {personId: $personId}) " +
                    "CREATE (l:Loan {loanId: $loanId, loanAmount: $loanAmount, balance: $balance, createTime: localDateTime($time)}) " +
                    "CREATE (l)<-[:apply {createTime: localDateTime($time)}]-(p)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w6);
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w7.toString());

            //Add Loan applied by Company
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("companyId", w7.getCompanyId()+"");
            queryParams.put("loanId", w7.getLoanId()+"");
            queryParams.put("time", DATE_FORMAT.format(w7.getTime()));
            queryParams.put("balance", w7.getBalance());
            queryParams.put("loanAmount", w7.getLoanAmount());

            /*
            String queryString = "MATCH (c:Company {companyId: $companyId}) " +
                    "MERGE (l:Loan {loanId: $loanId}) " +
                    "SET l.loanAmount = $loanAmount, " +
                    "l.balance = $balance, l.createTime = localDateTime($time) " +
                    "CREATE (l)<-[:apply {createTime: localDateTime($time)}]-(c)";
             */

            String queryString = "MATCH (c:Company {companyId: $companyId}) " +
                    "CREATE (l:Loan {loanId: $loanId, loanAmount: $loanAmount, balance: $balance, createTime: localDateTime($time)}) " +
                    "CREATE (l)<-[:apply {createTime: localDateTime($time)}]-(c)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w7);
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w8.toString());

            //Add Invest Between Person And Company
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("companyId", w8.getCompanyId()+"");
            queryParams.put("personId", w8.getPersonId()+"");
            queryParams.put("time", DATE_FORMAT.format(w8.getTime()));
            queryParams.put("ratio", w8.getRatio());

            String queryString = "MATCH (c:Company {companyId: $companyId}) " +
                    "MATCH (p:Person {personId: $personId}) " +
                    "CREATE (p)-[:invest {createTime: localDateTime($time), ratio: $ratio}]->(c)";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w8);
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w9.toString());

            //Add Invest Between Company And Company
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("companyId1", w9.getCompanyId1()+"");
            queryParams.put("companyId2", w9.getCompanyId2()+"");
            queryParams.put("time", DATE_FORMAT.format(w9.getTime()));
            queryParams.put("ratio", w9.getRatio());

            String queryString = "MATCH (c1:Company {companyId: $companyId1}) " +
                    "MATCH (c2:Company {companyId: $companyId2}) " +
                    "CREATE (c1)-[:invest {createTime: localDateTime($time), ratio: $ratio}]->(c2)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w9);
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w10.toString());

            //Add Guarantee Between Persons
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId1", w10.getPersonId1()+"");
            queryParams.put("personId2", w10.getPersonId2()+"");
            queryParams.put("time", DATE_FORMAT.format(w10.getTime()));

            String queryString = "MATCH (p1:Person {personId: $personId1}) " +
                    "MATCH (p2:Person {personId: $personId2}) " +
                    "CREATE (p1)-[:guarantee {createTime: localDateTime($time)}]->(p2)";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w10);
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w11.toString());

            //Add Guarantee Between Companies
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("companyId1", w11.getCompanyId1()+"");
            queryParams.put("companyId2", w11.getCompanyId2()+"");
            queryParams.put("time", DATE_FORMAT.format(w11.getTime()));

            String queryString = "MATCH (c1:Company {companyId: $companyId1}) " +
                    "MATCH (c2:Company {companyId: $companyId2}) " +
                    "CREATE (c1)-[:guarantee {createTime: localDateTime($time)}]->(c2)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w11);
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w12.toString());

            //Add Transfer Between Accounts
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId1", w12.getAccountId1()+"");
            queryParams.put("accountId2", w12.getAccountId2()+"");
            queryParams.put("time", DATE_FORMAT.format(w12.getTime()));
            queryParams.put("amount", w12.getAmount());

            String queryString = "MATCH (a1:Account {accountId: $accountId1}) " +
                    "MATCH (a2:Account {accountId: $accountId2}) " +
                    "CREATE (a1)-[:transfer {createTime: localDateTime($time), amount: $amount}]->(a2)";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w12);
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w13.toString());

            //Add Withdraw Between Accounts
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId1", w13.getAccountId1()+"");
            queryParams.put("accountId2", w13.getAccountId2()+"");
            queryParams.put("time", DATE_FORMAT.format(w13.getTime()));
            queryParams.put("amount", w13.getAmount());

            String queryString = "MATCH (a1:Account {accountId: $accountId1}) " +
                    "MATCH (a2:Account {accountId: $accountId2}) " +
                    "CREATE (a1)-[:withdraw {createTime: localDateTime($time), amount: $amount}]->(a2)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w13);
        }
    }

    public static class Write14Handler implements OperationHandler<Write14, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write14 w14, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w14.toString());

            //Add Repay Between Account And Loan
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w14.getAccountId()+"");
            queryParams.put("loanId", w14.getLoanId()+"");
            queryParams.put("time", DATE_FORMAT.format(w14.getTime()));
            queryParams.put("amount", w14.getAmount());

            String queryString = "MATCH (a:Account {accountId: $accountId}) " +
                    "MATCH (l:Loan {loanId: $loanId}) " +
                    "CREATE (a)-[:repay {createTime: localDateTime($time), amount: $amount}]->(l)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w14);
        }
    }

    public static class Write15Handler implements OperationHandler<Write15, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write15 w15, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w15.toString());

            //Add Deposit Between Loan And Account
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w15.getAccountId()+"");
            queryParams.put("loanId", w15.getLoanId()+"");
            queryParams.put("time", DATE_FORMAT.format(w15.getTime()));
            queryParams.put("amount", w15.getAmount());

            String queryString = "MATCH (a:Account {accountId: $accountId}) " +
                    "MATCH (l:Loan {loanId: $loanId}) " +
                    "CREATE (l)-[:deposit {createTime: localDateTime($time), amount: $amount}]->(a)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w15);
        }
    }

    public static class Write16Handler implements OperationHandler<Write16, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write16 w16, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w16.toString());

            //Account signed in with Medium
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w16.getAccountId()+"");
            queryParams.put("mediumId", w16.getMediumId()+"");
            queryParams.put("time", DATE_FORMAT.format(w16.getTime()));

            String queryString = "MATCH (a:Account {accountId: $accountId}) " +
                    "MATCH (m:Medium {mediumId: $mediumId}) " +
                    "CREATE (m)-[:signIn {createTime: localDateTime($time)}]->(a)";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w16);
        }
    }

    public static class Write17Handler implements OperationHandler<Write17, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write17 w17, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w17.toString());

            //Remove an Account
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w17.getAccountId()+"");

            String queryString = "MATCH (a:Account {accountId: $accountId}) " +
                    "OPTIONAL MATCH (a)-[:repay]->(loan:Loan)-[:deposit]->(a)" +
                    "DETACH DELETE a, loan";


            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w17);
        }
    }

    public static class Write18Handler implements OperationHandler<Write18, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write18 w18, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w18.toString());

            //Block an Account of high risk
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w18.getAccountId()+"");

            String queryString = "MATCH (a:Account {accountId: $accountId}) " +
                    "SET a.isBlocked = true";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w18);
        }
    }

    public static class Write19Handler implements OperationHandler<Write19, CypherDbConnectionState> {
        @Override
        public void executeOperation(Write19 w19, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w19.toString());

            //Block a Person of high risk
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w19.getPersonId()+"");

            String queryString = "MATCH (p:Person {personId: $personId}) " +
                    "SET p.isBlocked = true";

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w19);
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, CypherDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(rw1.toString());

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String simpleRead1String = "MATCH (account:Account {accountId: $id}) " +
                    "RETURN account.createTime AS createTime, account.isBlocked AS isBlocked, account.accountType AS type";
            HashMap<String, Object> queryParamsSR1Src = new HashMap<>();
            queryParamsSR1Src.put("id", rw1.getSrcId()+"");
            HashMap<String, Object> queryParamsSR1Dst = new HashMap<>();
            queryParamsSR1Dst.put("id", rw1.getDstId()+"");

            String resultSrc = client.execute(simpleRead1String, queryParamsSR1Src);
            String resultDst = client.execute(simpleRead1String, queryParamsSR1Dst);
            try {
                SimpleRead1Result[] simpleRead1SrcResults = new ObjectMapper().readValue(resultSrc, SimpleRead1Result[].class);
                SimpleRead1Result[] simpleRead1DstResults = new ObjectMapper().readValue(resultDst, SimpleRead1Result[].class);
                if(simpleRead1SrcResults.length>0 && simpleRead1SrcResults[0].getIsBlocked() || simpleRead1DstResults.length>0 && simpleRead1DstResults[0].getIsBlocked()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                    return;
                }
                String write12String = "MATCH (a1:Account {accountId: $accountId1}) " +
                        "MATCH (a2:Account {accountId: $accountId2}) " +
                        "CREATE (a1)-[:transfer {createTime: localDateTime($time), amount: $amount}]->(a2)";

                HashMap<String, Object> queryParamsW12 = new HashMap<>();
                queryParamsW12.put("accountId1", rw1.getSrcId()+"");
                queryParamsW12.put("accountId2", rw1.getDstId()+"");
                queryParamsW12.put("time", DATE_FORMAT.format(rw1.getTime()));
                queryParamsW12.put("amount", rw1.getAmount());

                Transaction tx = client.startTransaction(write12String, queryParamsW12);

                if(!tx.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                    return;
                }

                String complexRead4String = "MATCH " +
                        "(src:Account {accountId: $id1})-[edge1:transfer]->(dst:Account {accountId: $id2}), " +
                        "(dst)-[edge2:transfer]->(other:Account)-[edge3:transfer]->(src) " +                //changed src dst and direction so it matches cr4
                        "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                        "AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) " +
                        "AND localDateTime($start_time) < edge3.createTime < localDateTime($end_time) " +
                        "WITH " +
                        "other.accountId AS otherId, " +
                        "count(DISTINCT edge2) AS numEdge2, sum(DISTINCT edge2.amount) AS sumEdge2Amount, " +       //added distinct, so it doesn't add same edge more than once
                        "max(edge2.amount) AS maxEdge2Amount, " +
                        "count(DISTINCT edge3) AS numEdge3, sum(DISTINCT edge3.amount) AS sumEdge3Amount, " +
                        "max(edge3.amount) AS maxEdge3Amount " +
                        "ORDER BY sumEdge2Amount+sumEdge3Amount DESC " +
                        "WITH collect({otherId: otherId, numEdge2: numEdge2, sumEdge2Amount: sumEdge2Amount, " +
                        "maxEdge2Amount: maxEdge2Amount, numEdge3: numEdge3, sumEdge3Amount: sumEdge3Amount, " +
                        "maxEdge3Amount: maxEdge3Amount}) AS results " +
                        "WITH coalesce(head(results), {otherId: -1, numEdge2: 0, sumEdge2Amount: 0, maxEdge2Amount: 0, " +
                        "numEdge3: 0, sumEdge3Amount: 0, maxEdge3Amount: 0}) AS top " +
                        "RETURN " +
                        "top.otherId AS otherId, " +
                        "top.numEdge2 AS numEdge2,  " +
                        "top.sumEdge2Amount AS sumEdge2Amount, " +
                        "top.maxEdge2Amount AS maxEdge2Amount, " +
                        "top.numEdge3 AS numEdge3, " +
                        "top.sumEdge3Amount AS sumEdge3Amount, " +
                        "top.maxEdge3Amount AS maxEdge3Amount " +
                        "ORDER BY sumEdge2Amount DESC, sumEdge3Amount DESC, size(otherId) ASC, otherId ASC ";

                HashMap<String, Object> queryParamsCr4 = new HashMap<>();
                queryParamsCr4.put("id1", rw1.getSrcId()+"");
                queryParamsCr4.put("id2", rw1.getDstId()+"");
                queryParamsCr4.put("start_time", DATE_FORMAT.format(rw1.getStartTime()));
                queryParamsCr4.put("end_time", DATE_FORMAT.format(rw1.getEndTime()));

                Result result = tx.run(complexRead4String, queryParamsCr4);
                String resultCr4 = client.resultToString(result);
                ComplexRead4Result[] complexRead4Results = new ObjectMapper().readValue(resultCr4, ComplexRead4Result[].class);
                if (complexRead4Results.length == 0 || complexRead4Results[0].getOtherId() == -1) {
                    if(tx.isOpen()) tx.commit();
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
                    return;
                }
                if(tx.isOpen()) tx.rollback();

                String write18String = "MATCH (a:Account {accountId: $accountId}) " +
                        "SET a.isBlocked = true";

                HashMap<String, Object> queryParamsW18Src = new HashMap<>();
                queryParamsW18Src.put("accountId", rw1.getSrcId()+"");
                HashMap<String, Object> queryParamsW18Dst = new HashMap<>();
                queryParamsW18Dst.put("accountId", rw1.getDstId()+"");

                client.execute(write18String, queryParamsW18Src);
                client.execute(write18String, queryParamsW18Dst);


            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, CypherDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(rw2.toString());

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String simpleRead1String = "MATCH (account:Account {accountId: $id}) " +
                    "RETURN account.createTime AS createTime, account.isBlocked AS isBlocked, account.accountType AS type";
            HashMap<String, Object> queryParamsSR1Src = new HashMap<>();
            queryParamsSR1Src.put("id", rw2.getSrcId()+"");
            HashMap<String, Object> queryParamsSR1Dst = new HashMap<>();
            queryParamsSR1Dst.put("id", rw2.getDstId()+"");

            String resultSrc = client.execute(simpleRead1String, queryParamsSR1Src);
            String resultDst = client.execute(simpleRead1String, queryParamsSR1Dst);
            try {
                SimpleRead1Result[] simpleRead1SrcResults = new ObjectMapper().readValue(resultSrc, SimpleRead1Result[].class);
                SimpleRead1Result[] simpleRead1DstResults = new ObjectMapper().readValue(resultDst, SimpleRead1Result[].class);
                if(simpleRead1SrcResults.length>0 && simpleRead1SrcResults[0].getIsBlocked() || simpleRead1DstResults.length>0 && simpleRead1DstResults[0].getIsBlocked()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                    return;
                }
                String write12String = "MATCH (a1:Account {accountId: $accountId1}) " +
                        "MATCH (a2:Account {accountId: $accountId2}) " +
                        "CREATE (a1)-[:transfer {createTime: localDateTime($time), amount: $amount}]->(a2)";

                HashMap<String, Object> queryParamsW12 = new HashMap<>();
                queryParamsW12.put("accountId1", rw2.getSrcId()+"");
                queryParamsW12.put("accountId2", rw2.getDstId()+"");
                queryParamsW12.put("time", DATE_FORMAT.format(rw2.getTime()));
                queryParamsW12.put("amount", rw2.getAmount());

                Transaction tx = client.startTransaction(write12String, queryParamsW12);

                if(!tx.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                    return;
                }

                List<Float> ratios = new ArrayList<>();

                for(String id: new String[]{rw2.getSrcId()+"", rw2.getDstId()+""}) {


                    String complexRead7String = "MATCH (src:Account)-[edge1:transfer]->(mid:Account {accountId: $id})-[edge2:transfer]->(dst:Account)\n" +
                            "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) \n" +
                            "                    AND edge1.amount > $threshold \n" +
                            "                    AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) \n" +
                            "                    AND edge2.amount > $threshold \n" +
                            "WITH REDUCE(total = 0, account IN COLLECT(DISTINCT edge1) | total + account.amount) AS sumEdge1Amount, " +
                            "REDUCE(total = 0, account IN COLLECT(DISTINCT edge2) | total + account.amount) AS sumEdge2Amount,\n" +
                            "       COUNT(DISTINCT src) AS numSrc,\n" +
                            "       COUNT(DISTINCT dst) AS numDst\n" +
                            "RETURN numSrc, numDst," +
                            "       CASE " +
                            "       WHEN sumEdge2Amount > 0 THEN ROUND(1000 * sumEdge1Amount / sumEdge2Amount) / 1000 " +
                            "       ELSE -1 " +
                            "       END AS inOutRatio " +
                            "ORDER BY numSrc DESC, numDst DESC, inOutRatio DESC";

                    HashMap<String, Object> queryParamsCr7 = new HashMap<>();
                    queryParamsCr7.put("id", rw2.getSrcId()+"");
                    queryParamsCr7.put("start_time", DATE_FORMAT.format(rw2.getStartTime()));
                    queryParamsCr7.put("end_time", DATE_FORMAT.format(rw2.getEndTime()));
                    queryParamsCr7.put("threshold", rw2.getAmountThreshold());

                    String resultCr7 = client.execute(complexRead7String, queryParamsCr7);
                    ComplexRead7Result[] complexRead7Results = new ObjectMapper().readValue(resultCr7, ComplexRead7Result[].class);
                    if(complexRead7Results.length>0) ratios.add(complexRead7Results[0].getInOutRatio());
                }

                if (ratios.size()==2 && (ratios.get(0) <= rw2.getRatioThreshold() || ratios.get(1) <= rw2.getRatioThreshold())) {
                    if(tx.isOpen()) tx.commit();
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
                    return;
                }
                if(tx.isOpen()) tx.rollback();


                String write18String = "MATCH (a:Account {accountId: $accountId}) " +
                        "SET a.isBlocked = true";

                HashMap<String, Object> queryParamsW18Src = new HashMap<>();
                queryParamsW18Src.put("accountId", rw2.getSrcId()+"");
                HashMap<String, Object> queryParamsW18Dst = new HashMap<>();
                queryParamsW18Dst.put("accountId", rw2.getDstId()+"");

                client.execute(write18String, queryParamsW18Src);
                client.execute(write18String, queryParamsW18Dst);


            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, CypherDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, CypherDbConnectionState cypherDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(rw3.toString());

            CypherDbConnectionState.CypherClient client = cypherDbConnectionState.client();
            String simpleReadString = "MATCH (person:Person {personId: $id}) " +
                    "RETURN person.createTime AS createTime, person.isBlocked AS isBlocked, person.null AS type";               //type workaround so we can parse this query like simple read 1 even though it isn't
            HashMap<String, Object> queryParamsSRSrc = new HashMap<>();
            queryParamsSRSrc.put("id", rw3.getSrcId()+"");
            HashMap<String, Object> queryParamsSRDst = new HashMap<>();
            queryParamsSRDst.put("id", rw3.getDstId()+"");

            String resultSrc = client.execute(simpleReadString, queryParamsSRSrc);
            String resultDst = client.execute(simpleReadString, queryParamsSRDst);
            try {
                SimpleRead1Result[] simpleReadSrcResults = new ObjectMapper().readValue(resultSrc, SimpleRead1Result[].class);
                SimpleRead1Result[] simpleReadDstResults = new ObjectMapper().readValue(resultDst, SimpleRead1Result[].class);
                if(simpleReadSrcResults.length>0 && simpleReadSrcResults[0].getIsBlocked() || simpleReadDstResults.length>0 && simpleReadDstResults[0].getIsBlocked()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                    return;
                }
                String write10String = "MATCH (p1:Person {personId: $personId1}) " +
                        "MATCH (p2:Person {personId: $personId2}) " +
                        "CREATE (p1)-[:guarantee {createTime: localDateTime($time)}]->(p2)";

                HashMap<String, Object> queryParamsW10 = new HashMap<>();
                queryParamsW10.put("personId1", rw3.getSrcId()+"");
                queryParamsW10.put("personId2", rw3.getDstId()+"");
                queryParamsW10.put("time", DATE_FORMAT.format(rw3.getTime()));

                Transaction tx = client.startTransaction(write10String, queryParamsW10);

                if(!tx.isOpen()){
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                    return;
                }
                String complexRead11String = "MATCH path=(p1:Person {personId: $id})-[:guarantee*]->(pX:Person) " +
                        "WHERE all(e IN relationships(path) WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                        "UNWIND nodes(path)[1..] AS person " +
                        "MATCH (person)-[:apply]->(loan:Loan) " +
                        "RETURN " +
                        "(round(1000 * REDUCE(total = 0, loanA IN COLLECT(DISTINCT loan) | total + loanA.loanAmount))/1000) AS sumLoanAmount, " +
                        "count(DISTINCT loan) AS numLoans";

                HashMap<String, Object> queryParamsCr11 = new HashMap<>();
                queryParamsCr11.put("id", rw3.getSrcId()+"");
                queryParamsCr11.put("start_time", DATE_FORMAT.format(rw3.getStartTime()));
                queryParamsCr11.put("end_time", DATE_FORMAT.format(rw3.getEndTime()));

                String resultCr11 = client.execute(complexRead11String, queryParamsCr11);
                ComplexRead11Result[] complexRead11Results = new ObjectMapper().readValue(resultCr11, ComplexRead11Result[].class);


                if (complexRead11Results.length>0 && complexRead11Results[0].getSumLoanAmount()<=rw3.getThreshold()) {
                    if(tx.isOpen()) tx.commit();
                    resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
                    return;
                }
                if(tx.isOpen()) tx.rollback();


                String writeString = "MATCH (a:Person {personId: $personId}) " +
                        "SET a.isBlocked = true";

                HashMap<String, Object> queryParamsWSrc = new HashMap<>();
                queryParamsWSrc.put("personId", rw3.getSrcId()+"");
                HashMap<String, Object> queryParamsWDst = new HashMap<>();
                queryParamsWDst.put("personId", rw3.getDstId()+"");

                client.execute(writeString, queryParamsWSrc);
                client.execute(writeString, queryParamsWDst);


            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
        }
    }
}
