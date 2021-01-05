include ../ENV.list
export $(shell sed 's/=.*//' ../ENV.list)  

.PHONY: release, test, dev, run

release:
	cargo update
	cargo build --release
	cargo test
	strip target/release/pricing_microservice

build:
	cargo update
	cargo build
	cargo test

run:
	cargo run

dev:
	# . ./ENV.sh; backper
	cargo run;

test:
	cargo test