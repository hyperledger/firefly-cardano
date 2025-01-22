deploy-contract:
  cargo run -p firefly-cardano-deploy-contract -- --contract-path ./wasm/simple-tx --firefly-url http://localhost:5000
demo: deploy-contract
  cargo run -p firefly-cardano-demo -- --addr-from $ADDR_FROM --addr-to $ADDR_TO --amount 1000000

