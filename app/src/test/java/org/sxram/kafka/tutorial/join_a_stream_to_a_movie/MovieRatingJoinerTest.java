package org.sxram.kafka.tutorial.join_a_stream_to_a_movie;

import org.junit.jupiter.api.Test;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.Movie;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.RatedMovie;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.Rating;

import static org.junit.jupiter.api.Assertions.*;

/**
 * <a href="https://developer.confluent.io/tutorials/join-a-stream-to-a-table/kstreams.html">https://developer.confluent.io/tutorials/join-a-stream-to-a-table/kstreams.html</a>
 */
class MovieRatingJoinerTest {

    @Test
    void apply() {
        RatedMovie actualRatedMovie;

        Movie treeOfLife = Movie.newBuilder().setTitle("Tree of Life").setId(354).setReleaseYear(2011).build();
        Rating rating = Rating.newBuilder().setId(354).setRating(9.8).build();
        RatedMovie expectedRatedMovie = RatedMovie.newBuilder()
                .setTitle("Tree of Life")
                .setId(354)
                .setReleaseYear(2011)
                .setRating(9.8)
                .build();

        MovieRatingJoiner joiner = new MovieRatingJoiner();
        actualRatedMovie = joiner.apply(rating, treeOfLife);

        assertEquals(actualRatedMovie, expectedRatedMovie);
    }

}