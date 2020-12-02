ARG package_name=pricing_microservice
FROM debian:buster-slim
WORKDIR /usr/local/bin
COPY ./target/release/${package_name} /usr/local/bin/${package_name}
RUN apt-get update && apt-get install -y
RUN apt-get install curl -y
STOPSIGNAL SIGINT
ENTRYPOINT [${package_name}]