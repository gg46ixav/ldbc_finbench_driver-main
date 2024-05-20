RUN NEO4J
````bash
java -cp target/driver-0.2.0-alpha.jar org.ldbcouncil.finbench.driver.driver.Driver -P src/main/resources/example/ldbc_finbench_driver_neo4j.properties
````

RUN MEMGRAPH
````bash
java -cp target/driver-0.2.0-alpha.jar org.ldbcouncil.finbench.driver.driver.Driver -P src/main/resources/example/ldbc_finbench_driver_memgraph.properties
````

RUN JENA
````bash
java -cp target/driver-0.2.0-alpha.jar org.ldbcouncil.finbench.driver.driver.Driver -P src/main/resources/example/ldbc_finbench_driver_jena.properties
````

