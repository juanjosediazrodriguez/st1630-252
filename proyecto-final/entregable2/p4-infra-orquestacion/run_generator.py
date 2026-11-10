"""Ejecuta la CLI de P1 con las credenciales de entregable2/.env sin mostrarlas."""
from __future__ import annotations

import os
import sys

from deploy import E2, config


def main() -> None:
    cfg, _session = config()
    if cfg.get("AWS_ACCESS_KEY_ID") and cfg.get("AWS_SECRET_ACCESS_KEY"):
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            if cfg.get(name):
                os.environ[name] = cfg[name]
            else:
                os.environ.pop(name, None)
        os.environ.pop("AWS_PROFILE", None)
    elif cfg.get("AWS_PROFILE"):
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
            os.environ.pop(name, None)
        os.environ["AWS_PROFILE"] = cfg["AWS_PROFILE"]
    os.environ["AWS_DEFAULT_REGION"] = cfg["AWS_REGION"]
    sys.path.insert(0, str(E2 / "p1-generador"))
    from tiendacol_gen.cli import main as generator_main
    generator_main(sys.argv[1:])


if __name__ == "__main__":
    main()
