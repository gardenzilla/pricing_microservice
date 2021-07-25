FROM fedora:34
RUN dnf update -y && dnf clean all -y
WORKDIR /usr/local/bin
COPY ./target/release/pricing_microservice /usr/local/bin/pricing_microservice
STOPSIGNAL SIGINT
ENTRYPOINT ["pricing_microservice"]
