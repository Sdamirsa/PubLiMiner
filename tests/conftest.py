"""Shared pytest fixtures for PubLiMiner tests."""

from __future__ import annotations

from pathlib import Path

import pytest

SAMPLE_PUBMED_XML = """<?xml version="1.0"?>
<PubmedArticleSet>
<PubmedArticle>
  <MedlineCitation>
    <PMID Version="1">11111111</PMID>
    <Article>
      <ArticleTitle>Deep learning for medical imaging</ArticleTitle>
      <Abstract><AbstractText>We present a CNN approach for MRI segmentation.</AbstractText></Abstract>
      <AuthorList>
        <Author><LastName>Smith</LastName><ForeName>Jane</ForeName></Author>
      </AuthorList>
    </Article>
  </MedlineCitation>
</PubmedArticle>
<PubmedArticle>
  <MedlineCitation>
    <PMID Version="1">22222222</PMID>
    <Article>
      <ArticleTitle>Machine learning in oncology</ArticleTitle>
      <Abstract><AbstractText>Random forests predict tumor response.</AbstractText></Abstract>
      <AuthorList>
        <Author><LastName>Doe</LastName><ForeName>John</ForeName></Author>
      </AuthorList>
    </Article>
  </MedlineCitation>
</PubmedArticle>
</PubmedArticleSet>
"""


@pytest.fixture
def sample_xml() -> str:
    return SAMPLE_PUBMED_XML


@pytest.fixture
def tmp_output(tmp_path: Path) -> Path:
    out = tmp_path / "output"
    out.mkdir()
    return out
