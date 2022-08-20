package com.jpaucruz.github.topologies;

import com.jpaucruz.github.extractors.MovementTimestampExtractor;
import com.jpaucruz.github.model.Fraud;
import com.jpaucruz.github.model.Movement;
import com.jpaucruz.github.serializers.JsonDeserializer;
import com.jpaucruz.github.serializers.JsonSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Map;

@Service
public class FraudChecker {

    public static final String MOVEMENTS_TOPIC = "movements";
    public static final String FRAUD_TOPIC = "fraud-cases";
    public static final int ONLINE_MOVEMENT = 3;
    public static final int SESSION_WINDOW_DURATION_ONLINE_OPERATIONS = 60;
    Serde<Movement> movementSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Movement.class));
    Serde<Fraud> fraudSerde = Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(Fraud.class));

    @Autowired
    public void buildTopology(StreamsBuilder streamsBuilder) {

        // categorize movements
        Map<String, KStream<String, Movement>> movementTypes = streamsBuilder
                .stream(MOVEMENTS_TOPIC, Consumed.with(Serdes.String(), movementSerde).withTimestampExtractor(new MovementTimestampExtractor()))
                .map((k,v) -> KeyValue.pair(v.getCard(), v))
                .split(Named.as("type-"))
                .branch((k,v) -> v.getOrigin() == ONLINE_MOVEMENT, Branched.as("online"))
                .defaultBranch(Branched.as("physical"));
        //movementTypes.get("type-online").print(Printed.<String,Movement>toSysOut().withLabel("movements-online"));
        //movementTypes.get("type-physical").print(Printed.<String,Movement>toSysOut().withLabel("movements-physical"));

        KStream<String, Fraud> onlineFraudMovements = movementTypes.get("type-online")
                .groupByKey(Grouped.with(Serdes.String(), movementSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(SESSION_WINDOW_DURATION_ONLINE_OPERATIONS)))
                .aggregate(
                        Fraud::new,
                        (k, movement, fraudMovement) -> FraudCheckerOperations.aggMovement(movement, fraudMovement),
                        (k, i1, i2) -> FraudCheckerOperations.aggMerge(i1, i2),
                        Materialized.with(Serdes.String(), fraudSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                // check fraud (online movements)
                .filter((k,v) -> FraudCheckerOperations.isOnlineFraud(v))
                .map((k,v) -> KeyValue.pair(k.key(),v));
        //onlineFraudMovements.print(Printed.<String,Fraud>toSysOut().withLabel("fraud-online"));

        KStream<String, Fraud> physicalFraudMovements = movementTypes.get("type-physical")
                .groupByKey(Grouped.with(Serdes.String(), movementSerde))
                .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(SESSION_WINDOW_DURATION_ONLINE_OPERATIONS)))
                .aggregate(
                        Fraud::new,
                        (k, movement, fraudMovement) -> FraudCheckerOperations.aggMovement(movement, fraudMovement),
                        (k, i1, i2) -> FraudCheckerOperations.aggMerge(i1, i2),
                        Materialized.with(Serdes.String(), fraudSerde)
                )
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull()))
                .toStream()
                // check fraud (physical movements)
                .filter((k,v) -> FraudCheckerOperations.isPhysicalFraud(v))
                .map((k,v) -> KeyValue.pair(k.key(),v));
        //physicalFraudMovements.print(Printed.<String,Fraud>toSysOut().withLabel("fraud-physical"));

        // merge both types
        KStream<String, Fraud> fraudMovements = onlineFraudMovements.merge(physicalFraudMovements);
        //fraudMovements.print(Printed.<String,Fraud>toSysOut().withLabel("fraud"));
        fraudMovements.to(FRAUD_TOPIC, Produced.with(Serdes.String(), fraudSerde));

    }

}
