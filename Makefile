all: test build
test: lint audit
	cargo test
lint:
	cargo fmt --check
audit:
	cargo audit
build:
	cargo build --release

deploy-contract:
	cargo run -p firefly-cardano-deploy-contract -- --contract-path ./wasm/simple-tx
demo: deploy-contract
	cargo run -p firefly-cardano-demo -- --addr-from ${ADDR_FROM} --addr-to ${ADDR_TO} --amount 1000000
generate-key:
	cargo run --bin firefly-cardano-generate-key -- --wallet-dir infra/wallet --testnet

docker: docker-cardanoconnect docker-cardanosigner

docker-cardanoconnect:
	docker build --target firefly-cardanoconnect -t firefly-cardanoconnect .
docker-cardanosigner:
	docker build --target firefly-cardanosigner -t firefly-cardanosigner .