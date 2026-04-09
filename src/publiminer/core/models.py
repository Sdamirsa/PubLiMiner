"""Pydantic models for paper data — single progressive model."""

from __future__ import annotations

from pydantic import BaseModel, Field


class Author(BaseModel):
    """Author of a paper."""

    last_name: str = ""
    first_name: str = ""
    initials: str = ""
    affiliation: str = ""


class Journal(BaseModel):
    """Journal information."""

    title: str = ""
    title_abbreviated: str = ""
    iso_abbreviation: str = ""
    nlm_id: str = ""
    issn: str = ""
    issn_type: str = ""
    issn_linking: str = ""
    volume: str = ""
    issue: str = ""
    pagination: str = ""
    cited_medium: str = ""
    country: str = ""


class MeshHeading(BaseModel):
    """MeSH heading with optional qualifiers."""

    descriptor: str = ""
    descriptor_ui: str = ""
    descriptor_major: bool = False
    qualifiers: list[dict[str, str | bool]] = Field(default_factory=list)


class Keyword(BaseModel):
    """Keyword entry."""

    keyword: str = ""
    major: bool = False
    owner: str = ""


class Grant(BaseModel):
    """Grant information."""

    id: str = ""
    agency: str = ""
    country: str = ""
    acronym: str = ""


class PublicationType(BaseModel):
    """Publication type entry."""

    type: str = ""
    ui: str = ""


class PublicationDate(BaseModel):
    """Structured publication date."""

    year: int | None = None
    month: int | str | None = None
    day: int | str | None = None
    iso_date: str = ""


class ArticleId(BaseModel):
    """Article identifier (DOI, PII, PMC, etc.)."""

    id: str = ""
    type: str = ""
