deploy-contract:
  cargo run -p firefly-cardano-deploy-contract -- --contract-path ./wasm/simple-tx
demo: deploy-contract
  cargo run -p firefly-cardano-demo -- --addr-from $ADDR_FROM --addr-to $ADDR_TO --amount 1000000
generate-key:
  cargo run --bin firefly-cardano-generate-key -- --wallet-dir infra/wallet --testnet
