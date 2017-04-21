import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.GraknSession;
import ai.grakn.GraknTxType;
import ai.grakn.concept.Concept;
import ai.grakn.concept.ConceptId;
import ai.grakn.concept.ResourceType;
import ai.grakn.graql.Graql;
import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.MatchQuery;
import ai.grakn.graql.Var;
import ai.grakn.graql.analytics.ClusterQuery;
import ai.grakn.graql.analytics.DegreeQuery;

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
        Map<Long, Set<String>> degrees = degreeOfClusters();
        persistDegrees(degrees);
        System.out.println("Finished calculation!");
    }

    private static void persistDegrees(Map<Long, Set<String>> degrees) {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                // mutate the ontology
                Var degree = Graql.var().label("degree").sub("resource").datatype(ResourceType.DataType.LONG);
                Var cluster = Graql.var().label("cluster").has("degree");

                // execute the query
                graph.graql().insert(degree, cluster).execute();

                // don't forget to commit
                graph.commit();
            }

            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                // add the degrees to the cluster
                Set<Var> degreeMutation = new HashSet<>();
                degrees.forEach((degree, concepts) -> {
                    concepts.forEach(concept -> {
                        degreeMutation.add(Graql.var().id(ConceptId.of(concept)).has("degree",degree));
                    });
                });

                // execute the query
                graph.graql().insert(degreeMutation).execute();

                // don't forget to commit
                graph.commit();
            }
        }
    }

    private static Map<Long, Set<String>> degreeOfClusters() {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            // open a graph (database transaction)
            try (GraknGraph graph = session.open(GraknTxType.READ)) {

                // construct the analytics cluster query
                DegreeQuery query = graph.graql().compute().degree().in("cluster", "grouping").of("cluster");

                // execute the analytics query
                Map<Long, Set<String>> degrees = query.execute();

                return degrees;
            }
        }
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
                        insertVars.add(Graql.var().isa("grouping").rel("group", cluster).rel("member",memberVar));
                    });

                    // execute query and commit
                    graph.graql().insert(insertVars).execute();
                    graph.commit();
                }
            });
        }
    }

    private static void horrendousBugFixes() {
//        ch.qos.logback.classic.Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
//        rootLogger.setLevel(Level.toLevel("error"));
//        System.setProperty("hadoop.home.dir", "/");
    }

    private static void mutateOntology() {

        // initialise the connection to engine
        try (GraknSession session = Grakn.session(Grakn.DEFAULT_URI, "genealogy")) {

            // open a graph (database transaction)
            try (GraknGraph graph = session.open(GraknTxType.WRITE)) {

                // create set of vars representing the mutation
                Var group = Graql.var("group").label("group").sub("role");
                Var member = Graql.var("member").label("member").sub("role");
                Var grouping = Graql.var("grouping").label("grouping").sub("relation").relates(group).relates(member);
                Var cluster = Graql.var("cluster").label("cluster").sub("entity").plays(group);
                Var personPlaysRole = Graql.var("person").label("person").plays("member");
                Var marriagePlaysRole = Graql.var("marriage").label("marriage").plays("member");

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
