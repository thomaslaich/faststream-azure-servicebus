{ pkgs, lib, ... }:

{
  packages = [
    pkgs.git
    pkgs.just
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
