"""Tests for the PubMed XML parser."""

from __future__ import annotations

import re

from publiminer.steps.parse.xml_parser import parse_article_xml


def test_parse_article_xml(sample_xml):
    # Grab first <PubmedArticle>...</PubmedArticle> block
    match = re.search(r"<PubmedArticle>.*?</PubmedArticle>", sample_xml, re.DOTALL)
    assert match is not None
    parsed = parse_article_xml(match.group(0))

    assert parsed["pmid"] == "11111111"
    assert "Deep learning" in parsed["title"]
    assert "CNN" in parsed["abstract"]
    assert len(parsed["authors"]) == 1
    assert parsed["authors"][0]["last_name"] == "Smith"
