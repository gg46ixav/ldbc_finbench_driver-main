package org.ldbcouncil.finbench.impls.dummy;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

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

public class JenaDb extends Db {
    static Logger logger = LogManager.getLogger("JenaDb");

    private JenaDbConnectionState connectionState = null;
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

    @Override
    protected void onInit(Map<String, String> map, LoggingService loggingService) throws DbException {

        connectionState = new JenaDbConnectionState(map);
        logger.info("JenaDb initialized");

        // complex reads
        /*
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


         */
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


        /*
        registerOperationHandler(Write17.class, Write17Handler.class);
        registerOperationHandler(Write18.class, Write18Handler.class);
        registerOperationHandler(Write19.class, Write19Handler.class);


         */
        /*
        // read-writes
        registerOperationHandler(ReadWrite1.class, ReadWrite1Handler.class);
        registerOperationHandler(ReadWrite2.class, ReadWrite2Handler.class);
        registerOperationHandler(ReadWrite3.class, ReadWrite3Handler.class);

         */
    }

    @Override
    protected void onClose() throws IOException {
        logger.info("JenaDb closed");
    }

    @Override
    protected DbConnectionState getConnectionState() throws DbException {
        return connectionState;
    }

    public static class ComplexRead1Handler implements OperationHandler<ComplexRead1, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead1 cr1, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr1);

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr1.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr1.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr1.getEndTime()));

            String queryString = "MATCH p=(account:Account {accountId: $id})-[edge1:transfer*1..3]->(other:Account), "
                    + "(other)<-[edge2:signIn]-(medium:Medium {isBlocked: true}) "
                    + "WITH p, [e IN relationships(p) | e.createTime] AS ts, other, medium "
                    + "WHERE reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807 "
                    + "AND all(e IN edge1 WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) "
                    + "AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) "
                    + "RETURN other.accountId AS otherId, length(p) AS accountDistance, medium.mediumId AS mediumId, medium.mediumType AS mediumType "
                    + "ORDER BY accountDistance ASC";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead2Handler implements OperationHandler<ComplexRead2, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead2 cr2, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr2.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr2.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr2.getEndTime()));

            String queryString = "MATCH " +
                    "(person:Person {personId: $id})-[edge1:own]->(accounts:Account), " +
                    "p=(accounts)<-[edge2:transfer*1..3]-(other:Account), " +
                    "(other)<-[edge3:deposit]-(loan:Loan) " +
                    "WITH p, [e IN relationships(p) | e.createTime] AS ts, other, loan " +
                    "WHERE " +
                    "reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x ELSE 9223372036854775807 end) <> 9223372036854775807 " +
                    "AND all(e IN edge2 WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "AND localDateTime($start_time) < edge3.createTime < localDateTime($end_time) " +
                    "RETURN other.id AS otherId, sum(loan.amount) AS sumLoanAmount, sum(loan.balance) AS sumLoanBalance " +
                    "ORDER BY sumLoanAmount DESC";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead3Handler implements OperationHandler<ComplexRead3, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead3 cr3, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr3.getId1());
            queryParams.put("id2", cr3.getId2());
            queryParams.put("start_time", DATE_FORMAT.format(cr3.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr3.getEndTime()));

            String queryString = "MATCH path1=shortestPath((src:Account {accountId: $id1})-[edge:transfer*]->" +
                    "(dst:Account {accountId: $id2}))" +
                    "WHERE all(e IN edge WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "RETURN length(path1) AS shortestPathLength";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead4Handler implements OperationHandler<ComplexRead4, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead4 cr4, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr4.getId1());
            queryParams.put("id2", cr4.getId2());
            queryParams.put("start_time", DATE_FORMAT.format(cr4.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr4.getEndTime()));

            String queryString = "MATCH " +
                    "(src:Account {accountId: $id1})-[edge1:transfer]->(dst:Account {accountId: $id2}), " +
                    "(src)<-[edge2:transfer]-(other:Account)-[edge3:transfer]->(dst) " +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                    "AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) " +
                    "AND localDateTime($start_time) < edge3.createTime < localDateTime($end_time) " +
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

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead5Handler implements OperationHandler<ComplexRead5, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead5 cr5, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr5.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr5.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr5.getEndTime()));

            String queryString = "MATCH " +
                    "(person:Person {id: $id})-[edge1:own]->(src:Account), " +
                    "p=(src)-[edge2:transfer*1..3]->(dst:Account) " +
                    "WITH p, [e IN relationships(p) | e.timestamp] AS ts " +
                    "WHERE "+
                    "reduce(curr = head(ts), x IN tail(ts) | CASE WHEN curr < x THEN x " +
                    "ELSE 9223372036854775807 end) <> 9223372036854775807 " +
                    "AND all(e IN edge2 WHERE localDateTime($start_time) < e.timestamp < localDateTime($end_time)) " +
                    "RETURN p AS path " +
                    "ORDER BY length(p) DESC";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead6Handler implements OperationHandler<ComplexRead6, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead6 cr6, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr6.getId());
            queryParams.put("threshold1", cr6.getThreshold1());
            queryParams.put("threshold2", cr6.getThreshold2());
            queryParams.put("start_time", DATE_FORMAT.format(cr6.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr6.getEndTime()));

            String queryString = "MATCH (src1:Account)-[edge1:transfer]->(mid:Account)-[edge2:withdraw]-> " +
                    "(dstCard:Account {accountId: $id, type: 'card'}) " +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) AND edge1.amount > $threshold1 " +
                    "AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) AND edge2.amount > $threshold2 " +
                    "RETURN mid.id AS midId, sum(edge1.amount) AS sumEdge1Amount, " +
                    "sum(edge2.amount) AS sumEdge2Amount " +
                    "ORDER BY sumEdge2Amount DESC";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead7Handler implements OperationHandler<ComplexRead7, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead7 cr7, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr7.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr7.getId());
            queryParams.put("threshold", cr7.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr7.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr7.getEndTime()));

            String queryString = "MATCH (src:Account)-[edge1:transfer|withdraw]->(mid:Account {accountId: $id})-" +
                    "[edge2:transfer|withdraw]->(dst:Account)" +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                    "AND edge1.amount > $threshold " +
                    "AND localDateTime($start_time) < edge2.createTime < localDateTime($end_time) " +
                    "AND edge2.amount > $threshold " +
                    "WITH src, dst, edge1, edge2, sum(edge1.amount) AS sumEdge1Amount, sum(edge2.amount) AS sumEdge2Amount " +
                    "RETURN count(src) AS numSrc, count(dst) AS numDst, " +
                    "CASE " +
                    "WHEN sumEdge2Amount > 0 THEN round(1000*sumEdge1Amount/sumEdge2Amount) / 1000 " +
                    "ELSE 0 " +
                    "END AS inoutRatio";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead8Handler implements OperationHandler<ComplexRead8, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead8 cr8, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr8.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr8.getId());
            queryParams.put("threshold", cr8.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(cr8.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr8.getEndTime()));

            String queryString = "MATCH " +
                    "(loan:Loan {loanId: $id})-[edge1:deposit]->(src:Account), " +
                    "p=(src)-[edge234:transfer|withdraw*1..3]->(dst:Account) " +
                    "WITH loan, p, dst, [e IN relationships(p) | e.amount] AS amts " +
                    "WHERE " +
                    "$start_time < edge1.createTime < localDateTime($end_time) " +
                    "AND all(e IN edge234 WHERE localDateTime($start_time) < e.timestamp < localDateTime($end_time)) " +
                    "AND reduce(curr = head(amts), x IN tail(amts) | CASE WHEN (curr <> -1) " +
                    "AND (x > curr*$threshold) THEN x ELSE -1 end) <> -1 " +
                    "WITH loan, length(p)+1 AS distanceFromLoan, dst, sum(relationships(p)[-1].amount) AS inflow " +
                    "RETURN dst.id AS dstId, round(1000 * inflow/loan.loanAmount) / 1000 AS ratio, distanceFromLoan " +
                    "ORDER BY distanceFromLoan DESC, ratio DESC";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead9Handler implements OperationHandler<ComplexRead9, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead9 cr9, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr9.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr9.getId());
            queryParams.put("threshold", cr9.getThreshold());
            queryParams.put("lowerbound", 0);
            queryParams.put("upperbound", 2147483647);
            queryParams.put("start_time", DATE_FORMAT.format(cr9.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr9.getEndTime()));

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

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead10Handler implements OperationHandler<ComplexRead10, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead10 cr10, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr10.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id1", cr10.getPid1());
            queryParams.put("id2", cr10.getPid2());
            queryParams.put("start_time", DATE_FORMAT.format(cr10.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr10.getEndTime()));

            String queryString = "MATCH " +
                    "(p1:Person {personId: $id1})-[edge1:invest]->(m1:Company), " +
                    "(p2:Person {personId: $id2})-[edge2:invest]->(m2:Company) " +
                    "WHERE localDateTime($start_time) < edge1.timestamp < localDateTime($end_time) " +
                    "AND localDateTime($start_time) < edge2.timestamp < localDateTime($end_time) " +
                    "WITH gds.similarity.jaccard(collect(m1.id), collect(m2.id)) AS jaccardSimilarity " +
                    "RETURN round(1000 * jaccardSimilarity) / 1000 AS jaccardSimilarity";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead11Handler implements OperationHandler<ComplexRead11, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead11 cr11, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr11.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr11.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr11.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr11.getEndTime()));

            String queryString = "MATCH path=(p1:Person {personId: $id})-[:guarantee*]->(pX:Person) " +
                    "WHERE all(e IN relationships(path) WHERE localDateTime($start_time) < e.createTime < localDateTime($end_time)) " +
                    "UNWIND nodes(path)[1..] AS person " +
                    "MATCH (person)-[:apply]->(loan:Loan) " +
                    "RETURN sum(loan.loanAmount) AS sumLoanAmount, count(loan) AS numLoans";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class ComplexRead12Handler implements OperationHandler<ComplexRead12, JenaDbConnectionState> {
        @Override
        public void executeOperation(ComplexRead12 cr12, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(cr12.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", cr12.getId());
            queryParams.put("start_time", DATE_FORMAT.format(cr12.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(cr12.getEndTime()));

            String queryString = "MATCH (person:Person {personId: $id}) " +
                    "-[edge1:own]->(pAcc:Account) " +
                    "-[edge2:transfer]->(compAcc:Account) " +
                    "<-[edge3:own]-(company:Company) " +
                    "WHERE localDateTime($start_time) < edge2.createTime < localDateTime($end_time) " +
                    "RETURN compAcc.id AS compAccountId, sum(edge2.amount) AS sumEdge2Amount " +
                    "ORDER BY sumEdge2Amount DESC";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class SimpleRead1Handler implements OperationHandler<SimpleRead1, JenaDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead1 sr1, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr1.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr1.getId());

            String queryString = "MATCH (account:Account {accountId: $id}) " +
                    "RETURN account{.createTime,.isBlocked,.type}";

            /*
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX ex: <http://example.org/>

            SELECT ?account1 ?a ?b
            WHERE {

            ?account1 rdf:type ex:Account .
                FILTER(?account1 = ex:4638144666238200012)
            ?account1   ex:createTime ?a ;
                        ex:isBlocked ?b .
            }

             */
            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class SimpleRead2Handler implements OperationHandler<SimpleRead2, JenaDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead2 sr2, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr2.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr2.getId());
            queryParams.put("start_time", DATE_FORMAT.format(sr2.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr2.getEndTime()));

            String queryString = "MATCH (src:Account {accountId: $id}) " +
                    "OPTIONAL MATCH (src)-[edge1:transfer]->(dst1:Account) " +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                    "OPTIONAL MATCH (src)<-[edge2:transfer]->(dst2:Account) " +
                    "WHERE localDateTime($start_time) < edge2.createTime < localDateTime($end_time) " +
                    "RETURN " +
                    "    sum(edge1.amount), max(edge1.amount), count(edge1), " +
                    "    sum(edge2.amount), max(edge2.amount), count(edge2)";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class SimpleRead3Handler implements OperationHandler<SimpleRead3, JenaDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead3 sr3, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr3.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr3.getId());
            queryParams.put("threshold", sr3.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr3.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr3.getEndTime()));

            String queryString = "MATCH (src:Account)-[edge2:transfer]->(dst:Account {accountId: $id}) " +
                    "OPTIONAL MATCH (blockedSrc:Account {isBlocked: true})-[edge1:transfer]->(dst) " +
                    "WHERE localDateTime($start_time) < edge1.createTime < localDateTime($end_time) " +
                    "AND edge1.amount > $threshold " +
                    "RETURN " +
                    "CASE " +
                    "WHEN count(edge2) > 0 THEN " +
                    "round(1000*count(edge1)/count(edge2)) / 1000 " +
                    "ELSE 0 " +
                    "END AS blockRatio";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

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

    public static class SimpleRead4Handler implements OperationHandler<SimpleRead4, JenaDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead4 sr4, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr4.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr4.getId());
            queryParams.put("threshold", sr4.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr4.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr4.getEndTime()));

            String queryString = "MATCH (src:Account {accountId: $id})-[edge:transfer]->(dst:Account) " +
                    "WHERE localDateTime($start_time) < edge.createTime < localDateTime($end_time) " +
                    "AND edge.amount > $threshold " +
                    "RETURN dst.id AS dstId, count(edge) AS numEdges, sum(edge.amount) AS sumAmount";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead4Result> simpleRead4Results = null;
            try {
                simpleRead4Results = sr4.deserializeResult(result);
                resultReporter.report(simpleRead4Results.size(), simpleRead4Results, sr4);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), sr4);
            }
        }
    }

    public static class SimpleRead5Handler implements OperationHandler<SimpleRead5, JenaDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead5 sr5, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr5.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr5.getId());
            queryParams.put("threshold", sr5.getThreshold());
            queryParams.put("start_time", DATE_FORMAT.format(sr5.getStartTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr5.getEndTime()));

            String queryString = "MATCH (dst:Account {accountId: $id})<-[edge:transfer]-(src:Account) " +
                    "WHERE localDateTime($start_time) < edge.createTime < localDateTime($end_time) " +
                    "  AND edge.amount > $threshold " +
                    "RETURN src.id AS srcId, count(edge) AS numEdges, sum(edge.amount) AS sumAmount";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead5Result> simpleRead5Results = null;
            try {
                simpleRead5Results = sr5.deserializeResult(result);
                resultReporter.report(simpleRead5Results.size(), simpleRead5Results, sr5);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), sr5);
            }
        }
    }

    public static class SimpleRead6Handler implements OperationHandler<SimpleRead6, JenaDbConnectionState> {
        @Override
        public void executeOperation(SimpleRead6 sr6, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(sr6.toString());

            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("id", sr6.getId());
            queryParams.put("start_time", DATE_FORMAT.format(sr6.getStartTime().getTime()));
            queryParams.put("end_time", DATE_FORMAT.format(sr6.getEndTime().getTime()));

            String queryString = "MATCH (src:Account {accountId: $id})<-[e1:transfer]-(mid:Account)-[e2:transfer]->" +
                    "(dst:Account {isBlocked: true}) " +
                    "WHERE src.accountId <> dst.accountId " +
                    "  AND localDateTime($start_time) < e1.createTime < localDateTime($end_time) " +
                    "  AND localDateTime($start_time) < e2.createTime < localDateTime($end_time) " +
                    "RETURN collect(dst.accountId) AS dstId";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            String result = client.execute(queryString);

            List<SimpleRead6Result> simpleRead6Results = null;
            try {
                simpleRead6Results = sr6.deserializeResult(result);
                resultReporter.report(simpleRead6Results.size(), simpleRead6Results, sr6);
            } catch (IOException e) {
                //DummyDb.logger.warn(e.getMessage() + "\n" + cr1);
                resultReporter.report(0, new ArrayList<>(), sr6);
            }
        }
    }

    public static class Write1Handler implements OperationHandler<Write1, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write1 w1, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w1.toString());

            //Add a person Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "INSERT DATA{" +
                    "ex:" + w1.getPersonId() + " a ex:Person ;" +
                    "ex:personName \"" + w1.getPersonName() + "\" ;" +
                    "ex:isBlocked \"" + w1.getIsBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";


            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w1);
        }
    }

    public static class Write2Handler implements OperationHandler<Write2, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write2 w2, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w2.toString());

            //Add a Company Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "INSERT DATA{" +
                    "ex:" + w2.getCompanyId() + " a ex:Company ;" +
                    "ex:companyName \"" + w2.getCompanyName() + "\" ;" +
                    "ex:isBlocked \"" + w2.getIsBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w2);
        }
    }

    public static class Write3Handler implements OperationHandler<Write3, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write3 w3, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w3.toString());

            //Add a Medium Node

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "INSERT DATA{" +
                    "ex:" + w3.getMediumId() + " a ex:Medium ;" +
                    "ex:mediumType \"" + w3.getMediumType() + "\" ;" +
                    "ex:createTime \"" + DATE_FORMAT.format(new Date()) + "\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ." +
                    "}";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w3);
        }
    }

    public static class Write4Handler implements OperationHandler<Write4, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write4 w4, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w4.toString());

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w4.getPersonId() + " ex:ownPersonX ex:" + w4.getAccountId() + " >> " +
                    "                     ex:accountBlocked \"" + w4.getAccountBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ; " +
                    "                     ex:accountType \"" + w4.getAccountType() +"\" ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w4.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w4);
        }
    }

    public static class Write5Handler implements OperationHandler<Write5, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write5 w5, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w5.toString());

            //Add an Account Node owned by Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w5.getCompanyId() + " ex:ownCompanyX ex:" + w5.getAccountId() + " >> " +
                    "                     ex:accountBlocked \"" + w5.getAccountBlocked() + "\"^^<http://www.w3.org/2001/XMLSchema#boolean> ; " +
                    "                     ex:accountType \"" + w5.getAccountType() +"\" ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w5.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";


            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w5);
        }
    }

    public static class Write6Handler implements OperationHandler<Write6, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write6 w6, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w6.toString());

            //Add Loan applied by Person

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w6.getPersonId() + " ex:applyPerson ex:" + w6.getLoanId() + " >> " +
                    "                     ex:balance \"" + w6.getBalance() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:loanAmount \"" + w6.getLoanAmount() +"\" ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w6.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w6);
        }
    }

    public static class Write7Handler implements OperationHandler<Write7, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write7 w7, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w7.toString());

            //Add Loan applied by Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w7.getCompanyId() + " ex:applyCompanyX ex:" + w7.getLoanId() + " >> " +
                    "                     ex:balance \"" + w7.getBalance() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:loanAmount \"" + w7.getLoanAmount() +"\" ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w7.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";


            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w7);
        }
    }

    public static class Write8Handler implements OperationHandler<Write8, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write8 w8, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w8.toString());

            //Add Invest Between Person And Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w8.getPersonId() + " ex:investPersonX ex:" + w8.getCompanyId() + " >> " +
                    "                     ex:ratio \"" + w8.getRatio() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w8.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w8);
        }
    }

    public static class Write9Handler implements OperationHandler<Write9, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write9 w9, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w9.toString());

            //Add Invest Between Company And Company

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w9.getCompanyId1() + " ex:investCompanyX ex:" + w9.getCompanyId2() + " >> " +
                    "                     ex:ratio \"" + w9.getRatio() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w9.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";


            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w9);
        }
    }

    public static class Write10Handler implements OperationHandler<Write10, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write10 w10, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w10.toString());

            //Add Guarantee Between Persons
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("personId1", w10.getPersonId1());
            queryParams.put("personId2", w10.getPersonId2());
            queryParams.put("time", DATE_FORMAT.format(w10.getTime()));

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w10.getPersonId1() + " ex:guaranteePersonX ex:" + w10.getPersonId2() + " >> " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w10.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w10);
        }
    }

    public static class Write11Handler implements OperationHandler<Write11, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write11 w11, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w11.toString());

            //Add Guarantee Between Companies

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w11.getCompanyId1() + " ex:guaranteeCompanyX ex:" + w11.getCompanyId2() + " >> " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w11.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";


            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w11);
        }
    }

    public static class Write12Handler implements OperationHandler<Write12, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write12 w12, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w12.toString());

            //Add Transfer Between Accounts

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w12.getAccountId1() + " ex:transfer ex:" + w12.getAccountId2() + " >> " +
                    "                     ex:amount \"" + w12.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w12.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w12);
        }
    }

    public static class Write13Handler implements OperationHandler<Write13, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write13 w13, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w13.toString());

            //Add Withdraw Between Accounts

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w13.getAccountId1() + " ex:withdraw ex:" + w13.getAccountId2() + " >> " +
                    "                     ex:amount \"" + w13.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w13.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w13);
        }
    }

    public static class Write14Handler implements OperationHandler<Write14, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write14 w14, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w14.toString());

            //Add Repay Between Account And Loan

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w14.getAccountId() + " ex:repayX ex:" + w14.getLoanId() + " >> " +
                    "                     ex:amount \"" + w14.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w14.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w14);
        }
    }

    public static class Write15Handler implements OperationHandler<Write15, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write15 w15, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w15.toString());

            //Add Deposit Between Loan And Account

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w15.getLoanId() + " ex:depositX ex:" + w15.getAccountId() + " >> " +
                    "                     ex:amount \"" + w15.getAmount() + "\"^^<http://www.w3.org/2001/XMLSchema#float> ; " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w15.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";


            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w15);
        }
    }

    public static class Write16Handler implements OperationHandler<Write16, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write16 w16, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w16.toString());

            //Account signed in with Medium
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w16.getAccountId());
            queryParams.put("mediumId", w16.getMediumId());
            queryParams.put("time", DATE_FORMAT.format(w16.getTime()));

            String queryString = "PREFIX ex: <http://example.org/> " +
                    "            INSERT DATA{ " +
                    "                    << ex:" + w16.getMediumId() + " ex:signInX ex:" + w16.getAccountId() + " >> " +
                    "                     ex:createTime \""+ DATE_FORMAT.format(w16.getTime()) +"\"^^<http://www.w3.org/2001/XMLSchema#dateTime> . " +
                    "            }";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w16);
        }
    }

    public static class Write17Handler implements OperationHandler<Write17, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write17 w17, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w17.toString());

            //Remove an Account
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w17.getAccountId());

            String queryString = "MATCH (a:Account {accountId: $accountId}) " +
                    "OPTIONAL MATCH (a)-[:repay]->(loan:Loan)-[:deposit]->(a)" +
                    "DETACH DELETE a, loan";


            /*
            PREFIX ex: <http://example.org/>

            DELETE{
                <<?s ?p ?o>> ?a ?b .
            } WHERE {
                <<?s ?p ?o>> ?a ?b .
  	            FILTER(?s = ex:4894849844998308121 || ?o = ex:4894849844998308121)
            }
             */

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w17);
        }
    }

    public static class Write18Handler implements OperationHandler<Write18, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write18 w18, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w18.toString());

            //Block an Account of high risk
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w18.getAccountId());

            String queryString = "MATCH (a:Account {accountId: $accountId}) " +
                    "SET a.isBlocked = true";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w18);
        }
    }

    public static class Write19Handler implements OperationHandler<Write19, JenaDbConnectionState> {
        @Override
        public void executeOperation(Write19 w19, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(w19.toString());

            //Block a Person of high risk
            Map<String, Object> queryParams = new HashMap<>();
            queryParams.put("accountId", w19.getPersonId());

            String queryString = "MATCH (p:Person {personId: $personId}) " +
                    "SET p.isBlocked = true";

            JenaDbConnectionState.JenaClient client = jenaDbConnectionState.client();
            client.execute(queryString);
            resultReporter.report(0, LdbcNoResult.INSTANCE, w19);
        }
    }

    public static class ReadWrite1Handler implements OperationHandler<ReadWrite1, JenaDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite1 rw1, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(rw1.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, rw1);
        }
    }

    public static class ReadWrite2Handler implements OperationHandler<ReadWrite2, JenaDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite2 rw2, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {

            resultReporter.report(0, LdbcNoResult.INSTANCE, rw2);
        }
    }

    public static class ReadWrite3Handler implements OperationHandler<ReadWrite3, JenaDbConnectionState> {
        @Override
        public void executeOperation(ReadWrite3 rw3, JenaDbConnectionState jenaDbConnectionState,
                                     ResultReporter resultReporter) throws DbException {
            Neo4jDb.logger.info(rw3.toString());
            resultReporter.report(0, LdbcNoResult.INSTANCE, rw3);
        }
    }
}
