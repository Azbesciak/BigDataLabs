FROM openjdk:11.0-jre-slim-sid
RUN apt-get update && apt-get install sqlite3 -y
COPY unique_tracks.txt .
COPY triplets_sample_20p.txt .
COPY querier.jar .
WORKDIR .
ENTRYPOINT ["java", "-jar", "./code.jar",\
 "./example.db", "./unique_tracks.txt", "./triplets_sample_20p.txt"\
 ]

