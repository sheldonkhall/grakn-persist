import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.analytics.ClusterQuery;

import java.util.List;
import java.util.Map;

import static ai.grakn.graql.Graql.var;

/**
 *
 */
public class Main {

    public static void main(String[] args) {
        testConnection();
        computeClusters();
    }

    private static Map<String, Long> computeClusters() {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            // open a graph (database transaction)
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                // construct the analytics cluster query
                ClusterQuery<Map<String, Long>> query = graph.graql().compute().cluster().in("person", "marriage");

                // execute the analytics query
                Map<String, Long> clusters = query.execute();

                return clusters;
            }
        }
    }

    private static void testConnection() {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            // open a graph (database transaction)
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                // construct a match query to find people
                MatchQuery query = graph.graql().match(var("x").isa("person"));

                // execute the query
                List<Map<String, Concept>> result = query.limit(10).execute();

                // write the results to the console
                result.forEach(System.out::println);
            }
        }
    }
}
