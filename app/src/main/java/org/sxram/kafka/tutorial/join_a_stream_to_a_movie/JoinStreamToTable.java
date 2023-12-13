package org.sxram.kafka.tutorial.join_a_stream_to_a_movie;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.sxram.kafka.tutorial.App;
import org.sxram.kafka.tutorial.Utils;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.Movie;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.RatedMovie;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.Rating;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * <a href="https://developer.confluent.io/tutorials/join-a-stream-to-a-table/kstreams.html">https://developer.confluent.io/tutorials/join-a-stream-to-a-table/kstreams.html</a>
 */
public class JoinStreamToTable {

    public Topology buildTopology(Properties allProps) {
        final String movieTopic = allProps.getProperty("movie.topic.name");
        final String rekeyedMovieTopic = allProps.getProperty("rekeyed.movie.topic.name");
        final String ratingTopic = allProps.getProperty("rating.topic.name");
        final String ratedMoviesTopic = allProps.getProperty("rated.movies.topic.name");

        final StreamsBuilder builder = new StreamsBuilder();
        final MovieRatingJoiner joiner = new MovieRatingJoiner();

        // "The problem is that we can’t make any assumptions about the key of this stream, so we have to repartition it explicitly.
        // We use the map() method for that, creating a new KeyValue instance for each record, using the movie ID as the new key."
        KStream<String, Movie> movieStream = builder.<String, Movie>stream(movieTopic)
                .map((key, movie) -> new KeyValue<>(String.valueOf(movie.getId()), movie));

        movieStream.to(rekeyedMovieTopic);

        KTable<String, Movie> moviesTable = builder.table(rekeyedMovieTopic);

        // Creating the KStream<String,Rating> of ratings looks just like our first step with the movies: we create a stream
        // from the topic, then repartition it with the map() method. Note that we must choose the same key—movie ID—for our join to work.
        KStream<String, Rating> ratingsStream = builder.<String, Rating>stream(ratingTopic)
                .map((key, rating) -> new KeyValue<>(String.valueOf(rating.getId()), rating));

        // "When you join a stream and a table, you get a new stream, but you must be explicit about the value of that stream—the combination
        // between the value in the stream and the associated value in the table. The ValueJoiner interface in the Streams API does this work.
        // The single apply() method takes the stream and table values as parameters, and returns the value of the joined stream as output.
        // (Their keys are not a part of the equation, because they are equal by definition and do not change in the result.)"
        KStream<String, RatedMovie> ratedMovieStream = ratingsStream.join(moviesTable, joiner);

        ratedMovieStream.to(ratedMoviesTopic, Produced.with(Serdes.String(), ratedMovieAvroSerde(allProps)));

        return builder.build();
    }

    private SpecificAvroSerde<RatedMovie> ratedMovieAvroSerde(Properties allProps) {
        SpecificAvroSerde<RatedMovie> movieAvroSerde = new SpecificAvroSerde<>();
        movieAvroSerde.configure((Map)allProps, false);
        return movieAvroSerde;
    }

    public void createTopics(Properties allProps) {
        AdminClient client = AdminClient.create(allProps);
        List<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(
                allProps.getProperty("movie.topic.name"),
                Integer.parseInt(allProps.getProperty("movie.topic.partitions")),
                Short.parseShort(allProps.getProperty("movie.topic.replication.factor"))));

        topics.add(new NewTopic(
                allProps.getProperty("rekeyed.movie.topic.name"),
                Integer.parseInt(allProps.getProperty("rekeyed.movie.topic.partitions")),
                Short.parseShort(allProps.getProperty("rekeyed.movie.topic.replication.factor"))));

        topics.add(new NewTopic(
                allProps.getProperty("rating.topic.name"),
                Integer.parseInt(allProps.getProperty("rating.topic.partitions")),
                Short.parseShort(allProps.getProperty("rating.topic.replication.factor"))));

        topics.add(new NewTopic(
                allProps.getProperty("rated.movies.topic.name"),
                Integer.parseInt(allProps.getProperty("rated.movies.topic.partitions")),
                Short.parseShort(allProps.getProperty("rated.movies.topic.replication.factor"))));

        client.createTopics(topics);
        client.close();
    }

    public static void main(String[] args) {
        JoinStreamToTable ts = new JoinStreamToTable();
        Properties allProps = Utils.mergeProperties("config/" + App.CLIENT_CONFLUENT_PROPERTIES, "config/" + "join-a-stream-to-a-table.properties");
        allProps.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        allProps.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        Topology topology = ts.buildTopology(allProps);

        ts.createTopics(allProps);

        final KafkaStreams streams = new KafkaStreams(topology, allProps);
        final CountDownLatch latch = new CountDownLatch(1);

        // Attach shutdown handler to catch Control-C.
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close(Duration.ofSeconds(5));
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
