"""Verified Git effect values shared by frame integration and v2 events."""
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class CommitReceipt(BaseModel):
    """One linear commit and the exact paths it changes."""

    sha: str
    paths: list[str] = Field(default_factory=list)

    @field_validator("sha")
    @classmethod
    def _reject_blank_sha(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("commit sha must be non-blank")
        return value


class IntegrationReceipt(BaseModel):
    """A verified linear commit range and its disjoint-ownership path set."""

    commits: list[CommitReceipt] = Field(default_factory=list)

    @property
    def paths(self) -> list[str]:
        seen: dict[str, None] = {}
        for commit in self.commits:
            for path in commit.paths:
                seen.setdefault(path, None)
        return list(seen)
