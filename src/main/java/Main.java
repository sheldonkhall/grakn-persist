import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.analytics.ClusterQuery;
import ch.qos.logback.classic.Level;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static ai.grakn.graql.Graql.var;

/**
 *
 */
public class Main {

    public static void main(String[] args) {
        horrendousBugFixes();
        testConnection();
        Map<String, Set<String>> results = computeClusters();
        mutateOntology();
        persistClusters(results);
    }

    private static void persistClusters(Map<String, Set<String>> results) {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            // iterate through results of cluster query
            results.forEach((clusterID, memberSet) -> {

                // open a graph (database transaction)
                try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                    // collect the vars to insert
                    Set<Var> insertVars = new HashSet<>();

                    // create the cluster
                    Var cluster = Graql.var().isa("cluster");
                    insertVars.add(cluster);

                    // attach the members
                    memberSet.forEach(member -> {
                        Var memberVar = Graql.var().id(ConceptId.of(member));
                        insertVars.add(Graql.var().isa("grouping").rel("group",cluster).rel("member",memberVar));
                    });

                    // execute query and commit
                    graph.graql().insert(insertVars).execute();
                    graph.commit();
                }
            });
        }
    }

    private static void horrendousBugFixes() {
        ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.toLevel("off"));
        System.setProperty("hadoop.home.dir", "/");
    }

    private static void mutateOntology() {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            // open a graph (database transaction)
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                // create set of vars representing the mutation
                Var group = Graql.var("group").name("group").sub("role");
                Var member = Graql.var("member").name("member").sub("role");
                Var grouping = Graql.var("grouping").name("grouping").sub("relation").hasRole(group).hasRole(member);
                Var cluster = Graql.var("cluster").name("cluster").sub("entity").playsRole(group);
                Var personPlaysRole = Graql.var("person").name("person").playsRole("member");
                Var marriagePlaysRole = Graql.var("marriage").name("marriage").playsRole("member");

                // construct the insert query
                InsertQuery query = graph.graql().insert(group, member, grouping, cluster, personPlaysRole, marriagePlaysRole);

                // execute the insert query
                query.execute();

                // don't forget to commit the changes
                graph.commit();
            }
        }
    }

    private static Map<String, Set<String>> computeClusters() {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            // open a graph (database transaction)
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                // construct the analytics cluster query
                ClusterQuery<Map<String, Set<String>>> query = graph.graql().compute().cluster().in("person", "marriage").members();

                // execute the analytics query
                Map<String, Set<String>> clusters = query.execute();

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
