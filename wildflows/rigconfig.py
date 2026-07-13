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

    def build(self) -> Rig:
        return ShellRig(template=self.template)


class ScriptRigConfig(BaseModel):
    kind: Literal["script"] = "script"
    script: Path
    log_dir: Path
    timeout_s: float = 900.0
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
    """Parse + validate a rigs.yaml and build the RigRegistry it declares."""
    data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    parsed = RigsFile.model_validate(data)
    return RigRegistry({name: cfg.build() for name, cfg in parsed.rigs.items()})
