# Local environment configuration files. Can be used together with
# Direnv to quickly activate your development environment.
{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  packages = with pkgs; [
    gotools
    golangci-lint
  ];
}
