"""Owner-facing YAML configuration for root, resident, and one-shot frame rigs."""
from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, Union

import yaml
from pydantic import BaseModel, Field, field_validator

from wildflows.rig import EchoRig, Rig, RigRegistry, ScriptRig, ShellRig


class _RigConfigBase(BaseModel):
    description: str | None = None

    @field_validator("description")
    @classmethod
    def _single_line_description(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or "\n" in normalized or "\r" in normalized:
            raise ValueError("rig descriptions must be non-blank single lines")
        return normalized


class EchoRigConfig(_RigConfigBase):
    kind: Literal["echo"] = "echo"

    def build(self) -> Rig:
        return EchoRig()


class ShellRigConfig(_RigConfigBase):
    kind: Literal["shell"] = "shell"
    template: str
    timeout_s: float = Field(gt=0)  # required + positive — no unbounded/degenerate rig

    def build(self) -> Rig:
        return ShellRig(template=self.template, timeout_s=self.timeout_s)


class ScriptRigConfig(_RigConfigBase):
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
    descriptions: dict[str, str] = {}
    for name, config in parsed.rigs.items():
        if config.description is not None:
            descriptions[name] = config.description
        if isinstance(config, ScriptRigConfig):
            base = config_path.parent
            config = config.model_copy(update={
                "script": (base / config.script).resolve(),
                "log_dir": (base / config.log_dir).resolve(),
            })
        built[name] = config.build()
    return RigRegistry(built, descriptions)
