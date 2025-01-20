{
  description = "firefly-cardano";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixpkgs-unstable";

    devshell.url = "github:numtide/devshell";
    devshell.inputs.nixpkgs.follows = "";

    flake-parts.url = "github:hercules-ci/flake-parts";
    flake-parts.inputs.nixpkgs-lib.follows = "nixpkgs";

  };

  outputs = {...} @ inputs:
    inputs.flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "aarch64-linux" "x86_64-darwin" "aarch64-darwin"];
      imports = with inputs; [
        devshell.flakeModule
      ];

      perSystem = {
        pkgs,
        system,
        self',
        config,
        ...
      }: {
        devshells.default = {
          name = "firefly-cardano";
          imports = [
            "${inputs.devshell}/extra/language/rust.nix"
          ];

          packages = with pkgs; [
            alejandra
            lld
          ];

          commands = [
            {package = pkgs.treefmt;}
            {package = pkgs.just;}
          ];

          devshell.startup.setup.text = ''
            [ -e $PRJ_ROOT/.envrc.local ] && source $PRJ_ROOT/.envrc.local
          '';
        };
      };
    };
}
