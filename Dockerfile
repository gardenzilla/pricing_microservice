ARG package_name=pricing_microservice
FROM debian:buster-slim
WORKDIR /usr/local/bin
COPY ./target/release/pricing_microservice /usr/local/bin/pricing_microservice
RUN apt-get update && apt-get install -y
RUN apt-get install curl -y
STOPSIGNAL SIGINT
ENTRYPOINT ["pricing_microservice"]