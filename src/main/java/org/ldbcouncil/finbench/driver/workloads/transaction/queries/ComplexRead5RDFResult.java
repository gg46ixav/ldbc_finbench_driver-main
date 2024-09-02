package org.ldbcouncil.finbench.driver.workloads.transaction.queries;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Objects;
import org.ldbcouncil.finbench.driver.result.Path;

public class ComplexRead5RDFResult{
    public static final String START_ACCOUNT_ID = "startAccountId";
    private static final String OTHER_ACCOUNT_1_ID = "otherAccount1Id";
    private static final String OTHER_ACCOUNT_2_ID = "otherAccount2Id";
    private static final String OTHER_ACCOUNT_3_ID = "otherAccount3Id";
    private final Path path;

    public ComplexRead5RDFResult(@JsonProperty(START_ACCOUNT_ID) Long startAccountId,
                                 @JsonProperty(OTHER_ACCOUNT_1_ID) Long otherAccount1Id,
                                 @JsonProperty(OTHER_ACCOUNT_2_ID) Long otherAccount2Id,
                                 @JsonProperty(OTHER_ACCOUNT_3_ID) Long otherAccount3Id) {
        ArrayList<Long> pathList = new ArrayList<>();
        if(startAccountId!=null) {
            pathList.add(startAccountId);
            if (otherAccount1Id != null){
                pathList.add(otherAccount1Id);
                if (otherAccount2Id != null) {
                    pathList.add(otherAccount2Id);
                    if (otherAccount3Id != null) pathList.add(otherAccount3Id);
                }
            }
        }

        this.path = new Path(pathList);
    }

    public Path getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ComplexRead5RDFResult that = (ComplexRead5RDFResult) o;
        return Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public String toString() {
        return "ComplexRead5RDFResult{"
                + "path="
                + path
                + '}';
    }
}

