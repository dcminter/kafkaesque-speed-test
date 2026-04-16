#!/bin/bash
./mvnw test -Dtest=BrokerStartupTimingTest 2>&1 | grep -E '(\[TIMING\]|Tests run|BUILD|FAILURE|ERROR)'
