"""Layered Markdown skill discovery and prompt materialization."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from collections.abc import Iterable

__all__ = ["Skill", "SkillLibrary", "SkillLibraryError"]


class SkillLibraryError(ValueError):
    """A skill library entry or requested skill name is invalid."""


@dataclass(frozen=True)
class Skill:
    """One resolved frontmatter-free Markdown skill."""

    name: str
    description: str
    text: str
    source: Path


class SkillLibrary:
    """Repository skills layered over Wildflows' bundled stock library."""

    def __init__(self, repository: Path) -> None:
        bundled = Path(__file__).with_name("skills")
        repository_skills = Path(repository).resolve() / ".wildflows" / "skills"
        resolved = self._read_directory(bundled)
        # Repository-local definitions intentionally replace bundled skills with
        # the same filename stem.
        resolved.update(self._read_directory(repository_skills))
        self._skills = resolved

    @staticmethod
    def _read_directory(directory: Path) -> dict[str, Skill]:
        if not directory.is_dir():
            return {}
        skills: dict[str, Skill] = {}
        for path in sorted(directory.glob("*.md"), key=lambda item: item.name):
            text = path.read_text(encoding="utf-8")
            first_line = text.splitlines()[0] if text else ""
            heading = first_line.removeprefix("# ")
            title, separator, description = heading.partition(" — ")
            if (
                not first_line.startswith("# ")
                or not separator
                or not title.strip()
                or not description.strip()
            ):
                raise SkillLibraryError(
                    f"skill {path} must start with '# title — one-line description'"
                )
            name = path.stem
            if not name:
                raise SkillLibraryError(f"skill {path} has no filename stem")
            skills[name] = Skill(
                name=name,
                description=first_line.removeprefix("# ").strip(),
                text=text,
                source=path,
            )
        return skills

    @property
    def names(self) -> tuple[str, ...]:
        """Every resolved name in deterministic manifest order."""
        return tuple(sorted(self._skills))

    def resolve(self, names: Iterable[str]) -> list[Skill]:
        """Resolve an assigned bundle without reordering or deduplicating it."""
        resolved: list[Skill] = []
        for name in names:
            try:
                resolved.append(self._skills[name])
            except KeyError as exc:
                raise SkillLibraryError(f"unknown skill: {name!r}") from exc
        return resolved

    def manifest(self) -> str:
        """Render names and first-line descriptions for downward routing."""
        lines = ["SKILL MANIFEST:"]
        lines.extend(
            f"- {name}: {self._skills[name].description}" for name in self.names
        )
        return "\n".join(lines)
