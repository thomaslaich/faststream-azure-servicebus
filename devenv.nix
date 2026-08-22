{ pkgs, lib, ... }:

{
  packages = [
    pkgs.curl
    pkgs.git
    pkgs.just
    pkgs.jq
  ];

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

  process.manager = {
    implementation = "process-compose";
    after = "docker compose down --volumes --remove-orphans";
  };

  processes.servicebus-emulator = {
    exec = "docker compose up --abort-on-container-exit --remove-orphans";
    ready = {
      http.get = {
        port = 5300;
        path = "/health";
      };
      initial_delay = 2;
      period = 2;
      probe_timeout = 2;
      failure_threshold = 90;
      timeout = 180;
    };
  };

  treefmt = {
    enable = true;

    config = {
      programs = {
        just.enable = true;
        nixfmt.enable = true;
        ruff-format.enable = true;
        shellcheck.enable = true;
        shfmt.enable = true;
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

      settings.formatter.shellcheck.includes = [
        "*.sh"
        ".envrc"
        "scripts/*"
      ];

      settings.formatter.shfmt.includes = [
        "*.sh"
        ".envrc"
        "scripts/*"
      ];
    };
  };

  tasks."devenv:treefmt:run".exec = lib.mkForce null;
}
