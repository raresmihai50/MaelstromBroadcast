# Stage 1: Compilarea aplicației Java cu Maven
FROM maven:3.8.8-eclipse-temurin-17 AS builder
WORKDIR /build
# Copiem pom.xml și descărcăm dependențele (librăria JSON)
COPY pom.xml .
RUN mvn dependency:go-offline
# Copiem codul sursă și compilăm JAR-ul executabil
COPY src ./src
RUN mvn package -DskipTests

# Stage 2: Mediul de rulare Jepsen Maelstrom
FROM ubuntu:22.04
ENV DEBIAN_FRONTEND=noninteractive

# Instalăm dependențele necesare pentru Maelstrom
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    graphviz \
    gnuplot \
    wget \
    bzip2 \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /maelstrom

# Descărcăm Maelstrom v0.2.3
RUN wget https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 \
    && tar -xf maelstrom.tar.bz2 --strip-components=1 \
    && rm maelstrom.tar.bz2

# Aducem executabilul creat de Maven în stadiul 1
COPY --from=builder /build/target/maelstrom-1.0-SNAPSHOT.jar ./rb-node.jar

# Creăm o comandă rapidă /usr/local/bin/rb-node care va rula codul tău
RUN echo '#!/bin/bash\njava -jar /maelstrom/rb-node.jar "$@"' > /usr/local/bin/rb-node \
    && chmod +x /usr/local/bin/rb-node

# Lăsăm containerul deschis în linia de comandă
CMD ["/bin/bash"]