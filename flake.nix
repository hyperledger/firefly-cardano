{
  description = "firefly-cardano";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";

    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "";

    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";

    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "";
  };

  outputs = {...} @ inputs:
    inputs.flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "aarch64-linux" "aarch64-darwin"];
      imports = with inputs; [
        devshell.flakeModule
        treefmt-nix.flakeModule
      ];

      perSystem = {
        pkgs,
        system,
        self',
        config,
        ...
      }: {
        treefmt.programs = {
          alejandra.enable = true;
          rustfmt.enable = true;
        };

        devshells.default = {
          name = "firefly-cardano";
          imports = [
            "${inputs.devshell}/extra/language/rust.nix"
          ];

          packages = with pkgs; [
            just
          ];

          commands = [
            {package = config.treefmt.package;}
          ];

          devshell.startup.setup.text = ''
            [ -e $PRJ_ROOT/.envrc.local ] && source $PRJ_ROOT/.envrc.local
            cp -f ${config.treefmt.build.configFile} $PRJ_ROOT/treefmt.toml
          '';
        };
      };
    };
}
