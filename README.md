<img src="https://upload.wikimedia.org/wikipedia/commons/f/f8/Cardano.svg" width="250" align="right" height="90">
<!-- TODO: platform specific logo would be nice -->

# Monorepo for the Hyperledger Firefly implementation for Cardano blockchain

## Introduction

The Hyperledger Firefly monorepo for Cardano blockchain offers a unified repository containing all the code, documentation, and tools necessary for developing and deploying multi-enterprise blockchain applications on the Cardano network.

## Configuration of components

For the firefly connector config, see [config.md](firefly-cardanoconnect/config.md).

For the firefly signer config, see [config.md](firefly-cardanosigner/config.md).

## Getting started

To setup the components, you need a valid Blockfrost key, you can either [get it from the online service](https://blockfrost.io/) or [you can run your own cluster](https://github.com/blockfrost/blockfrost-backend-ryo).

### Run it with Docker compose

Requires at least Docker Compose version 2.22.0, uses Compose Watch: <https://docs.docker.com/compose/how-tos/file-watch/>

The easier way to get started is to use Docker compose to build your entire cluster.

```
# To compose a cluster using Blockfrost as a backing store
BLOCKFROST_KEY=previewXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX docker compose -f infra/docker-compose.yaml up --build -d

# To create a cluster with a local cardano node (takes longer to spin up)
docker compose -f infra/docker-compose.node.yaml up --build -d

# Watch the build
docker compose watch
```

### Demo application to showcase the Cardano Firefly connector

- Export your blockfrost key:

  ```
  export BLOCKFROST_KEY=previewXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
  ```

- Create Cardano wallet and put the signing key in `infra/wallet/${address}.skey`

  ```bash
    cargo run --bin firefly-cardano-generate-key -- --wallet-dir infra/wallet --testnet

    # or if you have installed just...
    just generate-key
  ```

- To start up the connector please execute:

  ```bash
  BLOCKFROST_KEY=previewXX docker compose -f ./infra/docker-compose.node.yaml -f ./infra/docker-compose.yaml -p preview up --build -d
  docker compose -f ./infra/docker-compose.yaml -p preview watch # Auto rebuild on changes
  ```

  > **_NOTE:_** If you want to avoid running it in the background, omit the `-d` flag.

  > **_NOTE:_** If you want to skip building, omit the `--build` flag.

- Swagger definitions can be viewed at `http://localhost:5018/api` and `http://localhost:8555/api`
- Execute `just demo` to run the demo application

## Engage with the community

- [Join us on Discord](https://discord.gg/hyperledger)
