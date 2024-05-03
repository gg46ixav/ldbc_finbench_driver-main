package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.ldbcouncil.finbench.driver.Db;
import org.ldbcouncil.finbench.driver.DbConnectionState;
import org.ldbcouncil.finbench.driver.DbException;
import org.ldbcouncil.finbench.driver.OperationHandler;
import org.ldbcouncil.finbench.driver.ResultReporter;
import org.ldbcouncil.finbench.driver.log.LoggingService;
import org.ldbcouncil.finbench.driver.workloads.transaction.LdbcNoResult;
import org.ldbcouncil.finbench.driver.workloads.transaction.queries.*;

public class DummyDb extends Db {
    static Logger logger = LogManager.getLogger("DummyDb");

    private DummyDbConnectionState connectionState = null;

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {

        String connectionUrl = "bolt://localhost:7687";
        connectionState = new DummyDbConnectionState(connectionUrl);
        logger.info("DummyDb initialized");

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

        registerOperationHandler(Write1.class, Write1Handler.class);

        registerOperationHandler(SimpleRead1.class, SimpleRead1Handler.class);
        registerOperationHandler(SimpleRead2.class, SimpleRead2Handler.class);
        registerOperationHandler(SimpleRead3.class, SimpleRead3Handler.class);

        /*

        // simple reads
        registerOperationHandler(SimpleRead4.class, SimpleRead4Handler.class);
        registerOperationHandler(SimpleRead5.class, SimpleRead5Handler.class);
        registerOperationHandler(SimpleRead6.class, SimpleRead6Handler.class);

        // writes

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

         */
    }

    @Override
    protected void onClose() throws IOException {
        logger.info("DummyDb closed");
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr1);

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr1.getId());
            queryParams.put("start_time", cr1.getStartTime().getTime());
            queryParams.put("end_time", cr1.getEndTime().getTime());

            String queryString = "MATCH p=(account:Account {accountId: $id})-[edge1:transfer*1..3]->(other:Account), "
                    + "(other)<-[edge2:signIn]-(medium:Medium {isBlocked: true}) "
                    + "WITH p, [e IN relationships(p) | e.createTime] AS ts, other, medium "
                    + "WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807 "
                    + "AND all(e IN edge1 WHERE $start_time < e.createTime < $end_time) "
                    + "AND $start_time < edge2.createTime < $end_time "
                    + "RETURN other.accountId AS otherId, length(p) AS accountDistance, medium.mediumId AS mediumId, medium.mediumType AS mediumType "
                    + "ORDER BY accountDistance ASC";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead1Result> complexRead1Results = null;
            try {
                complexRead1Results = cr1.deserializeResult(result);
                resultReporter.report(complexRead1Results.size(), complexRead1Results, cr1);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr1);
            }


        }
    }

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr2.getId());
            queryParams.put("start_time", cr2.getStartTime().getTime());
            queryParams.put("end_time", cr2.getEndTime().getTime());

            String queryString = "MATCH " +
                    "(person:Person {personId: $id})-[edge1:own]->(accounts:Account), " +
                    "p=(accounts)<-[edge2:transfer*1..3]-(other:Account), " +
                    "(other)<-[edge3:deposit]-(loan:Loan) " +
                    "WITH p, [e IN relationships(p) | e.createTime] AS ts, other, loan " +
                    "WHERE " +
                    "reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807 " +
                    "AND all(e IN edge2 WHERE $start_time < e.createTime < $end_time) " +
                    "AND $start_time < edge3.createTime < $end_time " +
                    "RETURN other.id AS otherId, sum(loan.amount) AS sumLoanAmount, sum(loan.balance) AS sumLoanBalance " +
                    "ORDER BY sumLoanAmount DESC";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead2Result> complexRead2Results = null;
            try {
                complexRead2Results = cr2.deserializeResult(result);
                resultReporter.report(complexRead2Results.size(), complexRead2Results, cr2);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr2);
            }
        }
    }

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr3.getId1());
            queryParams.put("id2", cr3.getId2());
            queryParams.put("start_time", cr3.getStartTime().getTime());
            queryParams.put("end_time", cr3.getEndTime().getTime());

            String queryString = "MATCH path1=shortestPath((src:Account {accountId: $id1})-[edge:transfer*]-> " +
                    "(dst:Account {accountId: $id2}))" +
                    "WHERE all(e IN edge WHERE $start_time < e.createTime < $end_time) " +
                    "RETURN length(path1) AS shortestPathLength";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead3Result> complexRead3Results = null;
            try {
                complexRead3Results = cr3.deserializeResult(result);
                resultReporter.report(complexRead3Results.size(), complexRead3Results, cr3);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr3);
            }
        }
    }

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr4.getId1());
            queryParams.put("id2", cr4.getId2());
            queryParams.put("start_time", cr4.getStartTime().getTime());
            queryParams.put("end_time", cr4.getEndTime().getTime());

            String queryString = "MATCH " +
                    "(src:Account {accountId: $id1})-[edge1:transfer]->(dst:Account {accountId: $id2}), " +
                    "(src)<-[edge2:transfer]-(other:Account)-[edge3:transfer]->(dst) " +
                    "WHERE $start_time < edge1.createTime < $end_time " +
                    "AND $start_time < edge2.createTime < $end_time " +
                    "AND $start_time < edge3.createTime < $end_time " +
                    "WITH " +
                    "other.id AS otherId, " +
                    "count(edge2) AS numEdge2, sum(edge2.amount) AS sumEdge2Amount, " +
                    "max(edge2.amount) AS maxEdge2Amount, " +
                    "count(edge3) AS numEdge3, sum(edge3.amount) AS sumEdge3Amount, " +
                    "max(edge3.amount) AS maxEdge3Amount " +
                    "ORDER BY sumEdge2Amount+sumEdge3Amount DESC " +
                    "WITH collect({otherId: otherId, numEdge2: numEdge2, sumEdge2Amount: sumEdge2Amount, " +
                    "maxEdge2Amount: maxEdge2Amount, numEdge3: numEdge3, sumEdge3Amount: sumEdge3Amount, " +
                    "maxEdge3Amount: maxEdge3Amount}) AS results " +
                    "WITH coalesce(head(results), {otherId: -1, numEdge2: 0, sumEdge2Amount: 0, maxEdge2Amount: 0, " +
                    "numEdge3: 0, sumEdge3Amount: 0, maxEdge3Amount: 0}) AS top " +
                    "RETURN top.otherId, top.numEdge2, top.sumEdge2Amount, top.maxEdge2Amount, top.numEdge3, " +
                    "top.sumEdge3Amount, top.maxEdge3Amount";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead4Result> complexRead4Results = null;
            try {
                complexRead4Results = cr4.deserializeResult(result);
                resultReporter.report(complexRead4Results.size(), complexRead4Results, cr4);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr4);
            }
        }
    }

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr5.getId());
            queryParams.put("start_time", cr5.getStartTime().getTime());
            queryParams.put("end_time", cr5.getEndTime().getTime());

            String queryString = "MATCH " +
                    "(person:Person {id: $id})-[edge1:own]->(src:Account), " +
                    "p=(src)-[edge2:transfer*1..3]->(dst:Account) " +
                    "WITH p, [e IN relationships(p) | e.timestamp] AS ts " +
                    "WHERE "+
                    "reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x " +
                    "ELSE 9223372036854775807 end) <> 9223372036854775807 " +
                    "AND all(e IN edge2 WHERE $start_time < e.timestamp < $end_time) " +
                    "RETURN p AS path " +
                    "ORDER BY length(p) DESC";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead5Result> complexRead5Results = null;
            try {
                complexRead5Results = cr5.deserializeResult(result);
                resultReporter.report(complexRead5Results.size(), complexRead5Results, cr5);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr5);
            }
        }
    }

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr6.getId());
            queryParams.put("threshold1", cr6.getThreshold1());
            queryParams.put("threshold2", cr6.getThreshold2());
            queryParams.put("start_time", cr6.getStartTime().getTime());
            queryParams.put("end_time", cr6.getEndTime().getTime());

            String queryString = "MATCH (src1:Account)-[edge1:transfer]->(mid:Account)-[edge2:withdraw]-> " +
                    "(dstCard:Account {accountId: $id, type: 'card'}) " +
                    "WHERE $start_time < edge1.createTime < $end_time AND edge1.amount > $threshold1 " +
                    "AND $start_time < edge2.createTime < $end_time AND edge2.amount > $threshold2 " +
                    "RETURN mid.id AS midId, sum(edge1.amount) AS sumEdge1Amount, " +
                    "sum(edge2.amount) AS sumEdge2Amount " +
                    "ORDER BY sumEdge2Amount DESC";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead6Result> complexRead6Results = null;
            try {
                complexRead6Results = cr6.deserializeResult(result);
                resultReporter.report(complexRead6Results.size(), complexRead6Results, cr6);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr6);
            }
        }
    }

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr7.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr7.getId());
            queryParams.put("threshold", cr7.getThreshold());
            queryParams.put("start_time", cr7.getStartTime().getTime());
            queryParams.put("end_time", cr7.getEndTime().getTime());

            String queryString = "MATCH (src:Account)-[edge1:transfer|withdraw]-> " +
                    "(mid:Account {accountId: $id})-[edge2:transfer|withdraw]->(dst:Account) " +
                    "WHERE $start_time < edge1.createTime < $end_time AND edge1.amount > $threshold " +
                    "AND $start_time < edge2.createTime < $end_time AND edge2.amount > $threshold " +
                    "RETURN count(src) AS numSrc, count(dst) AS numDst, " +
                    "CASE " +
                    "WHEN sum(edge2.amount) > 0 THEN " +
                    "round(1000*sum(edge1.amount)/sum(edge2.amount)) / 1000 " +
                    "ELSE 0 " +
                    "END AS inoutRatio";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead7Result> complexRead7Results = null;
            try {
                complexRead7Results = cr7.deserializeResult(result);
                resultReporter.report(complexRead7Results.size(), complexRead7Results, cr7);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr7);
            }
        }
    }

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr8.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr8.getId());
            queryParams.put("threshold", cr8.getThreshold());
            queryParams.put("start_time", cr8.getStartTime().getTime());
            queryParams.put("end_time", cr8.getEndTime().getTime());

            String queryString = "MATCH " +
                    "(loan:Loan {loanId: $id})-[edge1:deposit]->(src:Account), " +
                    "p=(src)-[edge234:transfer|withdraw*1..3]->(dst:Account) " +
                    "WITH loan, p, dst, [e IN relationships(p) | e.amount] AS amts " +
                    "WHERE " +
                    "$start_time < edge1.createTime < $end_time " +
                    "AND all(e IN edge234 WHERE $start_time < e.timestamp < $end_time) " +
                    "AND reduce(curr = head(amts), x IN tail(amts) | CASE WHEN (curr <> -1) " +
                    "AND (x > curr*$threshold) THEN x ELSE -1 end) <> -1 " +
                    "WITH loan, length(p)+1 AS distanceFromLoan, dst, sum(relationships(p)[-1].amount) AS inflow\n" +
                    "RETURN dst.id AS dstId, round(1000 * inflow/loan.loanAmount) / 1000 AS ratio, distanceFromLoan\n" +
                    "ORDER BY distanceFromLoan DESC, ratio DESC";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead8Result> complexRead8Results = null;
            try {
                complexRead8Results = cr8.deserializeResult(result);
                resultReporter.report(complexRead8Results.size(), complexRead8Results, cr8);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr8);
            }
        }
    }

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr9.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr9.getId());
            queryParams.put("threshold", cr9.getThreshold());
            queryParams.put("lowerbound", 0);
            queryParams.put("upperbound", 2147483647);
            queryParams.put("start_time", cr9.getStartTime().getTime());
            queryParams.put("end_time", cr9.getEndTime().getTime());

            String queryString = "MATCH " +
                    "(loan:Loan)-[edge1:deposit]->(mid:Account {accountId: $id})-[edge2:repay]->(loan), " +
                    "(up:Account)-[edge3:transfer]->(mid)-[edge4:transfer]->(down:Account) " +
                    "WHERE edge1.amount > $threshold AND $start_time < edge1.createTime < $end_time " +
                    "AND edge2.amount > $threshold AND $start_time < edge2.createTime < $end_time " +
                    "AND $lowerbound < edge1.amount/edge2.amount < $upperbound " +
                    "AND edge3.amount > $threshold AND $start_time < edge3.createTime < $end_time " +
                    "AND edge4.amount > $threshold AND $start_time < edge4.createTime < $end_time " +
                    "RETURN " +
                    "CASE " +
                    "WHEN sum(edge2.amount) > 0 AND sum(edge4.amount) > 0 THEN " +
                    "round(1000 * sum(edge1.amount)/sum(edge2.amount)) / 1000 " +
                    "ELSE 0 " +
                    "END AS ratioRepay, " +
                    "CASE " +
                    "WHEN sum(edge2.amount) > 0 THEN " +
                    "round(1000 * sum(edge1.amount)/sum(edge4.amount)) / 1000 " +
                    "ELSE 0 " +
                    "END AS ratioOut, " +
                    "CASE " +
                    "WHEN sum(edge4.amount) > 0 THEN " +
                    "round(1000 * sum(edge3.amount)/sum(edge4.amount)) / 1000 " +
                    "ELSE 0 " +
                    "END AS ratioIn";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead9Result> complexRead9Results = null;
            try {
                complexRead9Results = cr9.deserializeResult(result);
                resultReporter.report(complexRead9Results.size(), complexRead9Results, cr9);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr9);
            }
        }
    }

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr10.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr10.getPid1());
            queryParams.put("id2", cr10.getPid2());
            queryParams.put("start_time", cr10.getStartTime().getTime());
            queryParams.put("end_time", cr10.getEndTime().getTime());

            String queryString = "MATCH " +
                    "(p1:Person {personId: $id1})-[edge1:invest]->(m1:Company), " +
                    "(p2:Person {personId: $id2})-[edge2:invest]->(m2:Company) " +
                    "WHERE $start_time < edge1.timestamp < $end_time " +
                    "AND $start_time < edge2.timestamp < $end_time " +
                    "WITH gds.similarity.jaccard(collect(m1.id), collect(m2.id)) AS jaccardSimilarity " +
                    "RETURN round(1000 * jaccardSimilarity) / 1000 AS jaccardSimilarity";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead10Result> complexRead10Results = null;
            try {
                complexRead10Results = cr10.deserializeResult(result);
                resultReporter.report(complexRead10Results.size(), complexRead10Results, cr10);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr10);
            }
        }
    }

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr11.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr11.getId());
            queryParams.put("start_time", cr11.getStartTime().getTime());
            queryParams.put("end_time", cr11.getEndTime().getTime());

            String queryString = "MATCH path=(p1:Person {personId: $id})-[:guarantee*]->(pX:Person) " +
                    "WHERE all(e IN relationships(path) WHERE $start_time < e.createTime < $end_time) " +
                    "UNWIND nodes(path)[1..] AS person " +
                    "MATCH (person)-[:apply]->(loan:Loan) " +
                    "RETURN sum(loan.loanAmount) AS sumLoanAmount, count(loan) AS numLoans";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead11Result> complexRead11Results = null;
            try {
                complexRead11Results = cr11.deserializeResult(result);
                resultReporter.report(complexRead11Results.size(), complexRead11Results, cr11);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr11);
            }
        }
    }

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, DummyDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(cr12.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr12.getId());
            queryParams.put("start_time", cr12.getStartTime().getTime());
            queryParams.put("end_time", cr12.getEndTime().getTime());

            String queryString = "MATCH (person:Person {personId: $id}) " +
                    "-[edge1:own]->(pAcc:Account) " +
                    "-[edge2:transfer]->(compAcc:Account) " +
                    "<-[edge3:own]-(company:Company) " +
                    "WHERE $start_time < edge2.createTime < $end_time " +
                    "RETURN compAcc.id AS compAccountId, sum(edge2.amount) AS sumEdge2Amount " +
                    "ORDER BY sumEdge2Amount DESC";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<ComplexRead12Result> complexRead12Results = null;
            try {
                complexRead12Results = cr12.deserializeResult(result);
                resultReporter.report(complexRead12Results.size(), complexRead12Results, cr12);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), cr12);
            }
        }
    }

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr1.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr1.getId());

            String queryString = "MATCH (account:Account {accountId: $id}) " +
                    "RETURN account{.createTime,.isBlocked,.type}";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead1Result> simpleRead1Results = null;
            try {
                simpleRead1Results = sr1.deserializeResult(result);
                resultReporter.report(simpleRead1Results.size(), simpleRead1Results, sr1);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), sr1);
            }

        }
    }

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr2.getId());
            queryParams.put("start_time", sr2.getStartTime().getTime());
            queryParams.put("end_time", sr2.getEndTime().getTime());

            String queryString = "MATCH (src:Account {accountId: $id}) " +
                    "OPTIONAL MATCH (src)-[edge1:transfer]->(dst1:Account) " +
                    "WHERE $start_time < edge1.createTime < $end_time " +
                    "OPTIONAL MATCH (src)<-[edge2:transfer]->(dst2:Account) " +
                    "WHERE $start_time < edge2.createTime < $end_time " +
                    "RETURN " +
                    "    sum(edge1.amount), max(edge1.amount), count(edge1), " +
                    "    sum(edge2.amount), max(edge2.amount), count(edge2)";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead2Result> simpleRead2Results = null;
            try {
                simpleRead2Results = sr2.deserializeResult(result);
                resultReporter.report(simpleRead2Results.size(), simpleRead2Results, sr2);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), sr2);
            }

        }
    }

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr3.getId());
            queryParams.put("threshold", sr3.getThreshold());
            queryParams.put("start_time", sr3.getStartTime().getTime());
            queryParams.put("end_time", sr3.getEndTime().getTime());

            String queryString = "MATCH (src:Account)-[edge2:transfer]->(dst:Account {accountId: $id}) " +
                    "OPTIONAL MATCH (blockedSrc:Account {isBlocked: true})-[edge1:transfer]->(dst) " +
                    "WHERE $start_time < edge1.createTime < $end_time " +
                    "AND edge1.amount > $threshold " +
                    "RETURN " +
                    "CASE " +
                    "WHEN count(edge2) > 0 THEN " +
                    "round(1000*count(edge1)/count(edge2)) / 1000 " +
                    "ELSE 0 " +
                    "END AS blockRatio";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            String result = client.execute(queryString, queryParams);

            List<SimpleRead3Result> simpleRead3Results = null;
            try {
                simpleRead3Results = sr3.deserializeResult(result);
                resultReporter.report(simpleRead3Results.size(), simpleRead3Results, sr3);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), sr3);
            }
        }
    }

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr4.toString());
            resultReporter.report(0, Collections.EMPTY_LIST, sr4);
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr5.toString());
            resultReporter.report(0, Collections.EMPTY_LIST, sr5);
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, DummyDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(sr6.toString());
            resultReporter.report(0, Collections.EMPTY_LIST, sr6);
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w1.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId", w1.getPersonId());
            queryParams.put("personName", w1.getPersonName());
            queryParams.put("createTime", System.currentTimeMillis());
            queryParams.put("isBlocked", w1.getIsBlocked());

            String queryString = "CREATE (:Person {personId: $personId, personName: $personName," +
                    " isBlocked: $isBlocked, createTime: $createTime})";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w1);
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", w2.getCompanyId());
            queryParams.put("companyName", w2.getCompanyName());
            //queryParams.put("accountId", ?);
            queryParams.put("createTime", System.currentTimeMillis());
            queryParams.put("accountBlocked", w2.getIsBlocked());
            //queryParams.put("accountType", w2.get);

            String queryString = "CREATE (:Company {id: $companyId, name: $companyName})-[:own]-> " +
                    "(:Account {id: $accountId, createTime: $currentTime, isBlocked: $accountBlocked, " +
                    "type: $accountType})";

            DummyDbConnectionState.BasicClient client = dummyDbConnectionState.client();
            client.execute(queryString, queryParams);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w2);
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w3.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w3);
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w4.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w4);
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w5.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w5);
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w6.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w6);
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w7.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w7);
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w8.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w8);
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w9.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w9);
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w10.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w10);
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w11.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w11);
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w12.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w12);
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w13.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w13);
        }
    }

    public static class Write14Handler implements OperationHandler<Write14, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write14 w14, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w14.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w14);
        }
    }

    public static class Write15Handler implements OperationHandler<Write15, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write15 w15, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w15.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w15);
        }
    }

    public static class Write16Handler implements OperationHandler<Write16, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write16 w16, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w16.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w16);
        }
    }

    public static class Write17Handler implements OperationHandler<Write17, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write17 w17, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w17.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w17);
        }
    }

    public static class Write18Handler implements OperationHandler<Write18, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write18 w18, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w18.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w18);
        }
    }

    public static class Write19Handler implements OperationHandler<Write19, DummyDbConnectionState> {
        @Override
        public void executeOperation(Write19 w19, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(w19.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, w19);
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, DummyDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(rw1.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, DummyDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {

            resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, DummyDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, DummyDbConnectionState dummyDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            DummyDb.logger.info(rw3.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
        }
    }
}
