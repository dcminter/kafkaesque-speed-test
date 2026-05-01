# Kafkaesque Speed Test

A sample application used to determine a rough and ready speed comparison 
between [Kafkaesque](https://github.com/dcminter/kafkaesque) and other
test Kafka broker options (testcontainers and Embedded Kafka currently).

## Running the test

The script `speed.sh` run from the root of the project will build and
benchmark the performance of the integration test against the four 
underlying Kafka broker solutions.

Running the script on my 2017-era Dell laptop (a very underwhelming
piece of hardware) we see the following figures:

| Test Broker                        | Startup Duration | Shutdown Duration |
|------------------------------------|------------------|-------------------|
| KafkaesqueServer                   | 25 ms            | 39 ms             |
| KafkaesqueServer in TestContainers | 620 ms           | 360 ms            |
| EmbeddedKafkaBroker                | 1851 ms          | 3090 ms           |
| TestContainers Kafka               | 1958 ms          | 251 ms            |

Running on a much more modern Ryzen 9 system the startup durations are, 
respectively, around 11ms, 346ms, 669ms, and 346ms - so there everything's
about three times as fast.

Running in-process Kafkaesque looks great. Running under testcontainers it's
still looking good - and some of that will be Testcontainers, Ryuk, Docker 
overhead rather than Kafkaesque per se.
