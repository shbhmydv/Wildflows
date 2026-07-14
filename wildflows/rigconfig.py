"""Owner-facing rig configuration: a YAML `rigs.yaml` -> a validated RigRegistry.

YAML by policy for owner-facing config. Each named rig is a Pydantic-validated,
discriminated union on `kind` (echo | shell | script); an unknown kind or a missing
per-kind field is rejected at load time. `load_rigs(path) -> RigRegistry` builds the
concrete rigs the engine resolves at execution time.
"""
from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, Union

import yaml
from pydantic import BaseModel, Field

from wildflows.rig import EchoRig, Rig, RigRegistry, ScriptRig, ShellRig


class EchoRigConfig(BaseModel):
    kind: Literal["echo"] = "echo"

    def build(self) -> Rig:
        return EchoRig()


class ShellRigConfig(BaseModel):
    kind: Literal["shell"] = "shell"
    template: str
    timeout_s: float = Field(gt=0)  # required + positive — no unbounded/degenerate rig

    def build(self) -> Rig:
        return ShellRig(template=self.template, timeout_s=self.timeout_s)


class ScriptRigConfig(BaseModel):
    kind: Literal["script"] = "script"
    script: Path
    log_dir: Path
    timeout_s: float = Field(default=900.0, gt=0)
    env: dict[str, str] = Field(default_factory=dict)
    busy_patterns: list[str] | None = None

    def build(self) -> Rig:
        return ScriptRig(
            script=self.script,
            log_dir=self.log_dir,
            timeout_s=self.timeout_s,
            env=self.env,
            busy_patterns=self.busy_patterns,
        )


RigConfig = Annotated[
    Union[EchoRigConfig, ShellRigConfig, ScriptRigConfig],
    Field(discriminator="kind"),
]


class RigsFile(BaseModel):
    """The parsed rigs.yaml: name -> rig config."""

    rigs: dict[str, RigConfig]


def load_rigs(path: Path) -> RigRegistry:
    """Parse a rigs.yaml; relative script/log paths are relative to that file."""
    config_path = Path(path).resolve()
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    parsed = RigsFile.model_validate(data)
    built: dict[str, Rig] = {}
    for name, config in parsed.rigs.items():
        if isinstance(config, ScriptRigConfig):
            base = config_path.parent
            config = config.model_copy(update={
                "script": (base / config.script).resolve(),
                "log_dir": (base / config.log_dir).resolve(),
            })
        built[name] = config.build()
    return RigRegistry(built)
