 
.PHONY: release, test, dev

release:
	cargo update
	cargo build --release
	strip target/release/pricing_microservice

build:
	cargo build

dev:
	# . ./ENV.sh; backper
	cargo run;

test:
	cargo test