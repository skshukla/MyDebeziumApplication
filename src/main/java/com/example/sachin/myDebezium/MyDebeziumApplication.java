package com.example.sachin.myDebezium;

import com.example.sachin.myDebezium.serdes.JsonDeserializer;
import com.example.sachin.myDebezium.serdes.JsonSerializer;
import com.example.sachin.myDebezium.vo.Address;
import com.example.sachin.myDebezium.vo.User;
import com.example.sachin.myDebezium.vo.UserAddressJoin;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

@SpringBootApplication
@Slf4j
public class MyDebeziumApplication {

    private static final String APP_ID = "app-stream-000-000-";

    private static Properties props = null;
    protected Gson GSON = new GsonBuilder()
            .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
            .create();

    private Gson GSON_TO_PRINT = new GsonBuilder()
            .setPrettyPrinting()
            .create();

    private static final String KAFKA_BROKERS = "kube0:30092";

    @Value("${topics.user}")
    private String USER_TOPIC;

    @Value("${topics.user-address}")
    private String USER_ADDRESS_TOPIC;

    public static void main(String[] args) {
        SpringApplication.run(MyDebeziumApplication.class, args);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void run() throws Exception {
//        this.kTableTokStreamJoinExample();
        this.kTableTokStreamJoinExample2();
    }


    private void printStream(final KStream<Integer, ?> stream) {
        stream.foreach((k, v) -> {
            log.info("Printing ====> Key [{}], Value [{}]", k, GSON_TO_PRINT.toJson(v));
        });
    }

    private void printStreamWindowed(final KStream<Windowed<Integer>, ?> stream) {
        stream.foreach((k, v) -> {
            log.info("Windowed Printing ====> Key [{}], Value [{}]", k, GSON_TO_PRINT.toJson(v));
        });
    }

    private void kTableTokStreamJoinExample2() throws Exception {
        log.info("Starting the streaming application with APP_ID as [{}]", APP_ID);
        log.info("Entering method {run()}, with USER_TOPIC val as {{}}, USER_ADDRESS_TOPIC val as {{}}", USER_TOPIC, USER_ADDRESS_TOPIC);
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<Integer, User> sUser = this.getUserAsStream(streamsBuilder);
        final KStream<Integer, Address> ksAddress = this.getAddressAsStream(streamsBuilder);
        final KGroupedStream<Integer, User> sGroupedUser = sUser.groupByKey(Grouped.with(Serdes.Integer(), getSerde(new User())));

//        final SessionWindowedKStream<Integer, User> sUserSessionWindowed = sGroupedUser.windowedBy(SessionWindows.with(Duration.ofSeconds(2)).grace(Duration.ofSeconds(1)));
        final TimeWindowedKStream<Integer, User> sUserTimeWindowed = sGroupedUser.windowedBy(TimeWindows.of(Duration.ofSeconds(2)).grace(Duration.ofSeconds(1)));



//        final Materialized<Integer, User, KeyValueStore<Bytes, byte[]>> materialized =
//                Materialized.<Integer, User, KeyValueStore<Bytes, byte[]>>as("aggregate-store-2").withValueSerde(getSerde(new User()));

        final Materialized<Integer, User, WindowStore<Bytes, byte[]>> materializedTimed =
                Materialized.<Integer, User, WindowStore<Bytes, byte[]>>as("aggregate-store-timed-user").withValueSerde(getSerde(new User()));

        final KTable<Windowed<Integer>, User> ktUserWindowed = sUserTimeWindowed.aggregate(() -> new User(), (k, v, a) -> {
            return v;
        }, materializedTimed).suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
                        .withName("suppress-window"));


//        final KGroupedStream<Integer, Address> sGroupedUserAddress = ksAddress.groupByKey(Grouped.with(Serdes.Integer(), getSerde(new Address())));
//
//        final KTable<Integer, UserAddressJoin> kUserAddresses = sGroupedUserAddress.aggregate(() -> new UserAddressJoin(), (k, v, a) -> {
//            a.setId(k);
//            a.addAddress(v);
//            return a;
//        }, Materialized.<Integer, UserAddressJoin, KeyValueStore<Bytes, byte[]>>as("aggregate-store-3").withValueSerde(getSerde(new UserAddressJoin())));
//
//
//        final KTable<Integer, UserAddressJoin> kUserAddressesResult = kUserAddresses.join(ktUser, (userAddressJoin, user) -> {
//            userAddressJoin.setEmail(user.getEmail());
//            userAddressJoin.setFirstName(user.getFirstName());
//            userAddressJoin.setLastName(user.getLastName());
//            return userAddressJoin;
//        });


        this.printStreamWindowed(ktUserWindowed.toStream());


        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), loadProperties());
        streams.start();
        log.info("Going to sleep!!!!");
        Thread.sleep(5 * 60 * 1000);
        log.info("Exiting method CountStreamExample#run()......");
        streams.close();
    }


    private void kTableTokStreamJoinExample() throws Exception {
        log.info("Starting the streaming application with APP_ID as [{}]", APP_ID);
        log.info("Entering method {run()}, with USER_TOPIC val as {{}}, USER_ADDRESS_TOPIC val as {{}}", USER_TOPIC, USER_ADDRESS_TOPIC);
        final StreamsBuilder streamsBuilder = new StreamsBuilder();
        final KStream<Integer, User> sUser = this.getUserAsStream(streamsBuilder);
        final KStream<Integer, Address> ksAddress = this.getAddressAsStream(streamsBuilder);

        final KGroupedStream<Integer, User> sGroupedUser = sUser.groupByKey(Grouped.with(Serdes.Integer(), getSerde(new User())));

        final KTable<Integer, User> ktUser = sGroupedUser.aggregate(() -> new User(), (k, v, a) -> {
            return v;
        }, Materialized.<Integer, User, KeyValueStore<Bytes, byte[]>>as("aggregate-store-2").withValueSerde(getSerde(new User())));


        final KStream<Integer, UserAddressJoin> ksUserAddressJoin = ksAddress.join(ktUser, (address, user) -> {
            return UserAddressJoin.getInstance(user, address);
        }, Joined.<Integer, Address, User>with(Serdes.Integer(), getSerde(new Address()), getSerde(new User())));

        printStream(ksUserAddressJoin);


        final KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), loadProperties());
        streams.start();
        log.info("Going to sleep!!!!");
        Thread.sleep(5 * 60 * 1000);
        log.info("Exiting method CountStreamExample#run()......");
        streams.close();
    }

    private KStream<Integer, User> getUserAsStream(final StreamsBuilder streamsBuilder) {
        final KStream<String, String> sUserOrig = streamsBuilder.stream(USER_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        return sUserOrig.map((k, v) -> {
            final JSONObject jsonObject = new JSONObject(v);
            final JSONObject innerObj = (JSONObject) jsonObject.get("after");
            final User user = GSON.fromJson(innerObj.toString(), User.class);
            return KeyValue.pair(Integer.parseInt(innerObj.get("id").toString()), user);
        });
    }

    private KStream<Integer, Address> getAddressAsStream(final StreamsBuilder streamsBuilder) {
        final KStream<String, String> sUserAddressOrig = streamsBuilder.stream(USER_ADDRESS_TOPIC, Consumed.with(Serdes.String(), Serdes.String()));
        return sUserAddressOrig
                .filter((k, v) -> {
                    return !Objects.isNull(v) && !String.valueOf((new JSONObject(v)).get("after")).equals("null");
                })
                .map((k, v) -> {
                    final JSONObject jsonObject = new JSONObject(v);
                    final JSONObject innerObj = (JSONObject) jsonObject.get("after");
                    final Address address = GSON.fromJson(innerObj.toString(), Address.class);
                    return KeyValue.pair(Integer.parseInt(innerObj.get("user_id").toString()), address);
                });
    }


    private static Serde<List<Address>> getListAddressSerde() {
        final JsonSerializer<List<Address>> serializer = new JsonSerializer<>();
        final JsonDeserializer<List<Address>> deserializer = new JsonDeserializer<List<Address>>(List.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }


    private static <T> Serde<T> getSerde(T t) {
        final JsonSerializer<T> serializer = new JsonSerializer<>();
        final JsonDeserializer<T> deserializer = new JsonDeserializer<T>(t.getClass());
        return Serdes.serdeFrom(serializer, deserializer);
    }

    private synchronized Properties loadProperties() {
        if (props != null) {
            return props;
        }
        org.apache.kafka.common.serialization.Serdes ssdf;
        props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 3000);
        return props;
    }
}
