# Local environment configuration files. Can be used together with
# Direnv to quickly activate your development environment.
{pkgs ? import <nixpkgs> {}}:
pkgs.mkShell {
  packages = with pkgs; [
    gotools
    golangci-lint
    goperf
  ];
  shellHook = ''
  go install golang.org/x/pkgsite/cmd/pkgsite@latest
  export pkgsite=$HOME/go/bin/pkgsite
  '';
}
