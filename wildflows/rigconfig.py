"""Owner-facing YAML configuration for root, resident, and one-shot frame rigs."""
from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal, Union

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator

from wildflows.rig import EchoRig, Rig, RigRegistry, ScriptRig, ShellRig


class _RigConfigBase(BaseModel):
    description: str | None = None
    slots: int | None = Field(default=None, strict=True, gt=0)
    gate_timeout_s: float | None = Field(default=None, gt=0)

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


class WorktreeConfig(BaseModel):
    """Repository-wide provisioning applied to each fresh frame checkout."""

    setup: str | None = None
    link: list[str] = Field(default_factory=list)

    @field_validator("setup")
    @classmethod
    def _nonblank_setup(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value.strip():
            raise ValueError("worktree setup command must be non-blank")
        return value

    @field_validator("link")
    @classmethod
    def _relative_links(cls, values: list[str]) -> list[str]:
        normalized: list[str] = []
        for value in values:
            candidate = Path(value)
            if (
                not value.strip()
                or candidate.is_absolute()
                or candidate in (Path("."), Path(".."))
                or ".." in candidate.parts
                or ".git" in candidate.parts
            ):
                raise ValueError(
                    "worktree links must be repository-relative paths outside .git"
                )
            clean = candidate.as_posix()
            if clean in normalized:
                raise ValueError("worktree links must not contain duplicates")
            normalized.append(clean)
        return normalized


class RigsFile(BaseModel):
    """The parsed rigs.yaml: rigs plus optional notification and kind defaults."""

    rigs: dict[str, RigConfig]
    notify: str | None = None
    kinds: dict[str, str] = Field(default_factory=dict)
    worktree: WorktreeConfig = Field(default_factory=WorktreeConfig)

    @field_validator("kinds")
    @classmethod
    def _valid_kind_mappings(cls, value: dict[str, str]) -> dict[str, str]:
        normalized: dict[str, str] = {}
        for kind, rig in value.items():
            clean_kind = kind.strip()
            clean_rig = rig.strip()
            if (
                not clean_kind
                or not clean_rig
                or "\n" in clean_kind
                or "\r" in clean_kind
                or "\n" in clean_rig
                or "\r" in clean_rig
            ):
                raise ValueError("kind mappings must use non-blank single lines")
            normalized[clean_kind] = clean_rig
        return normalized

    @model_validator(mode="after")
    def _known_kind_rigs(self) -> "RigsFile":
        unknown = set(self.kinds.values()) - set(self.rigs)
        if unknown:
            raise ValueError(
                f"kinds map to unknown rigs: {', '.join(sorted(unknown))}"
            )
        return self

    @field_validator("notify")
    @classmethod
    def _nonblank_notify(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or "\n" in normalized or "\r" in normalized:
            raise ValueError("notify command must be a non-blank single line")
        return normalized


def load_rigs_config(path: Path) -> tuple[RigRegistry, str | None]:
    """Parse rigs and run options; resolve rig paths relative to the YAML file."""
    config_path = Path(path).resolve()
    data = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    parsed = RigsFile.model_validate(data)
    built: dict[str, Rig] = {}
    descriptions: dict[str, str] = {}
    slots: dict[str, int] = {}
    gate_timeouts: dict[str, float] = {}
    for name, config in parsed.rigs.items():
        if config.description is not None:
            descriptions[name] = config.description
        if config.slots is not None:
            slots[name] = config.slots
        if config.gate_timeout_s is not None:
            gate_timeouts[name] = config.gate_timeout_s
        if isinstance(config, ScriptRigConfig):
            base = config_path.parent
            config = config.model_copy(update={
                "script": (base / config.script).resolve(),
                "log_dir": (base / config.log_dir).resolve(),
            })
        built[name] = config.build()
    return RigRegistry(
        built,
        descriptions,
        slots=slots,
        kinds=parsed.kinds,
        gate_timeouts=gate_timeouts,
        worktree_setup=parsed.worktree.setup,
        worktree_links=parsed.worktree.link,
    ), parsed.notify


def load_rigs(path: Path) -> RigRegistry:
    """Parse a rigs.yaml while preserving the registry-only compatibility API."""
    registry, _ = load_rigs_config(path)
    return registry
