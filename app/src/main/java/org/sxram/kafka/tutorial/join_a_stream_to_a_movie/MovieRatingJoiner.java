package org.sxram.kafka.tutorial.join_a_stream_to_a_movie;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.Movie;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.RatedMovie;
import org.sxram.kafka.tutorial.join_a_stream_to_a_movie.avro.Rating;

/**
 * https://developer.confluent.io/tutorials/join-a-stream-to-a-table/kstreams.html
 */
public class MovieRatingJoiner implements ValueJoiner<Rating, Movie, RatedMovie> {

    public RatedMovie apply(Rating rating, Movie movie) {
        return RatedMovie.newBuilder()
                .setId(movie.getId())
                .setTitle(movie.getTitle())
                .setReleaseYear(movie.getReleaseYear())
                .setRating(rating.getRating())
                .build();
    }

}
