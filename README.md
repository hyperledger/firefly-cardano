# firefly-cardano


## Getting Set Up

Install Nix:
```console
# Install Nix
curl --proto '=https' --tlsv1.2 -sSf -L https://install.determinate.systems/nix | sh -s -- install

# Enter devshell:
nix develop
```

### (Optional) Install direnv
Should you prefer to load the devshell automatically when in a terminal

- Install Direnv:
  ```
  # Install direnv
  nix profile install nixpkgs#direnv

  # Configure your shell to load direnv everytime you enter this project (If you do not use bash see: https://direnv.net/docs/hook.html)
  echo 'eval "$(direnv hook bash)"' >> ~/.bashrc

  # And in case your system does not automatically .bashrc
  echo 'eval "$(direnv hook bash)"' >> ~/.bash_profile

  # Configure blockfrost key
  cp .envrc.local.example .envrc.local && vi .envrc.local
  ```

- Renter the shell for direnv to take effect
- Trust direnv config (.envrc), whitelisting is required whenever this file changes
  ```
  direnv allow
  ```
## Play with it!

- Export blockfrost key (done automaticaly by direnv if you have it set up):
  ```
  export BLOCKFROST_KEY=previewXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
  ```
- Create Cardano wallet and put the signing key in `infra/wallet/${address}.skey`
  ```
    cardano-cli address key-gen --verification-key-file firefly.vkey --signing-key-file firefly.skey
    cardano-cli address build --payment-verification-key-file firefly.vkey --out-file firefly.addr
    mkdir -p infra/wallet
    cp firefly.skey infra/wallet/$(cat firefly.addr).skey
    rm firefly.vkey firefly.skey firefly.addr
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

## Architecture

![](./arch.svg)

## Config

For the firefly connector config, see [config.md](firefly-cardanoconnect/config.md).

For the firefly signer config, see [config.md](firefly-cardanosigner/config.md).
