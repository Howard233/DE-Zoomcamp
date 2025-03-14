from pyflink.common.time import Duration
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    DataTypes,
    EnvironmentSettings,
    StreamTableEnvironment,
    TableEnvironment,
)


def create_trips_sink(t_env):
    table_name = "processed_trips_aggregated"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            PULocationID INT,
            DOLocationID INT,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            session_duration BIGINT,
            trip_count BIGINT,
            PRIMARY KEY (PULocationID, DULocationID, window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_green_trips_source(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime TIMESTAMP(3),
            lpep_dropoff_datetime TIMESTAMP(3),
            PULocationID INT,
            DOLocationID INT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_time AS TO_TIMESTAMP(lpep_dropoff_datetime),
            WATERMARK for event_time as event_time - INTERVAL '5' SECONDS
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:9092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def find_longest_sessions():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(5)
    ).with_timestamp_assigner(
        # This lambda is your timestamp assigner:
        #   event -> The data record
        #   timestamp -> The previously assigned (or default) timestamp
        lambda event, timestamp: event[
            2
        ]  # We treat the second tuple element as the event-time (ms).
    )
    try:
        # Create Kafka table
        source_table = create_green_trips_source(t_env)
        aggregated_table = create_trips_sink(t_env)

        t_env.execute_sql(
            f"""
        INSERT INTO {aggregated_table}
        SELECT
            PULocationID,
            DOLocationID,
            window_start,
            window_end,
            TIMESTAMPDIFF(SECOND, window_start, window_end) AS session_duration,
            COUNT(*) AS trip_count
        FROM TABLE(
            TUMBLE(TABLE {source_table}, DESCRIPTOR(event_watermark), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID, DOLocationID;
        
        """
        ).wait()

    except Exception as e:
        print("Writing records from Kafka to JDBC failed:", str(e))


if __name__ == "__main__":
    find_longest_sessions()
