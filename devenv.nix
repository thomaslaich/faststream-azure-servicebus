{
  pkgs,
  lib,
  config,
  ...
}:

{
  packages = [
    pkgs.git
    pkgs.just
    pkgs.process-compose
  ];

  env = {
    PC_CONFIG_FILES = "${config.devenv.root}/process-compose.yaml";
    PC_SOCKET_PATH = "${config.devenv.runtime}/process-compose.sock";
  };

  languages.python = {
    enable = true;
    package = pkgs.python313;

    venv.enable = true;

    uv = {
      enable = true;
      sync = {
        enable = true;
        groups = [ "dev" ];
      };
    };
  };

  treefmt = {
    enable = true;

    config = {
      programs = {
        just.enable = true;
        nixfmt.enable = true;
        ruff-format.enable = true;
        taplo.enable = true;
        yamlfmt.enable = true;
      };

      settings.excludes = [
        ".devenv/**"
        ".direnv/**"
        ".git/**"
        ".venv/**"
        "dist/**"
        "htmlcov/**"
      ];
    };
  };

  tasks."devenv:treefmt:run".exec = lib.mkForce null;
}
