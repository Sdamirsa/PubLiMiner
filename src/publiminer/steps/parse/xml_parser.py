"""PubMed XML parser — extract structured data from PubMed article XML.

Migrated from AI-in-Med-Trend/Code/S2_Pubmed_XML_Cleaner.py
and AI-in-Med-Trend/Code/S2_Prepare_and_ManualLabels.py.

All XML extraction methods preserved. LLM input prep and exclusion
flagging integrated as post-parse enrichments.
"""

from __future__ import annotations

import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any

logger = logging.getLogger("publiminer.parse")


def clean_xml_string(xml_string: str) -> str:
    """Clean XML string to handle potential parsing issues.

    Args:
        xml_string: Raw XML string.

    Returns:
        Cleaned XML string.
    """
    # Remove invalid XML characters
    xml_string = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", xml_string)

    # Ensure XML declaration is present
    if not xml_string.strip().startswith("<?xml"):
        xml_string = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_string

    return xml_string


def parse_article_xml(article_xml: str) -> dict[str, Any]:
    """Parse a single PubmedArticle XML string into a structured dict.

    This wraps the article in a PubmedArticleSet if needed, then delegates
    to the element-based parser.

    Args:
        article_xml: XML string for a single PubmedArticle.

    Returns:
        Dict of parsed article data.
    """
    # Strip invalid chars but don't add declaration yet
    xml_str = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", article_xml).strip()

    # Strip any pre-existing XML declaration so wrapping is safe
    xml_str = re.sub(r"^<\?xml[^?]*\?>\s*", "", xml_str)

    # Wrap in root element if needed
    if "<PubmedArticleSet>" not in xml_str:
        xml_str = f"<PubmedArticleSet>{xml_str}</PubmedArticleSet>"

    try:
        root = ET.fromstring(xml_str)
        article_elem = root.find(".//PubmedArticle")
        if article_elem is None:
            return {}
        return _parse_pubmed_article(article_elem)
    except ET.ParseError as e:
        logger.error(f"XML parsing error: {e}")
        return {}


def parse_batch_xml(xml_string: str) -> list[dict[str, Any]]:
    """Parse a batch XML string containing multiple PubmedArticle elements.

    Args:
        xml_string: Full PubMed XML response.

    Returns:
        List of parsed article dicts.
    """
    xml_str = clean_xml_string(xml_string)

    try:
        root = ET.fromstring(xml_str)
        articles = []
        for article_elem in root.findall(".//PubmedArticle"):
            data = _parse_pubmed_article(article_elem)
            if data:
                articles.append(data)
        return articles
    except ET.ParseError as e:
        logger.error(f"Batch XML parsing error: {e}")
        return []


def _parse_pubmed_article(article_elem: ET.Element) -> dict[str, Any]:
    """Parse a PubmedArticle XML element.

    Args:
        article_elem: ET.Element for a PubmedArticle.

    Returns:
        Dict with all extracted article fields.
    """
    article: dict[str, Any] = {}

    # PMID
    pmid_elem = article_elem.find(".//PMID")
    if pmid_elem is not None and pmid_elem.text:
        article["pmid"] = pmid_elem.text
    else:
        return {}

    # MedlineCitation data
    medline = article_elem.find(".//MedlineCitation")
    if medline is not None:
        article.update(_extract_article_info(medline))
        mesh = _extract_mesh_headings(medline)
        if mesh:
            article["mesh_headings"] = mesh
        keywords = _extract_keywords(medline)
        if keywords:
            article["keywords"] = keywords
        journal = _extract_journal_info(medline)
        if journal:
            article["journal"] = journal

    # PubmedData (DOI, article IDs, history)
    pubmed_data = article_elem.find(".//PubmedData")
    if pubmed_data is not None:
        article.update(_extract_pubmed_data(pubmed_data))

    return article


def _extract_article_info(medline: ET.Element) -> dict[str, Any]:
    """Extract article info from MedlineCitation."""
    info: dict[str, Any] = {}
    article_elem = medline.find("./Article")
    if article_elem is None:
        return info

    # Title
    title_elem = article_elem.find("./ArticleTitle")
    if title_elem is not None:
        # Get all text including tail of child elements
        info["title"] = _get_element_text(title_elem)

    # Abstract
    abstract = _extract_abstract(article_elem)
    if abstract:
        info["abstract"] = abstract

    # Authors
    authors = _extract_authors(article_elem)
    if authors:
        info["authors"] = authors

    # Publication types
    pub_types = _extract_publication_types(article_elem)
    if pub_types:
        info["publication_types"] = pub_types

    # Publication date
    pub_date = _extract_publication_date(article_elem)
    if pub_date:
        info["publication_date"] = pub_date

    # Language
    lang_elem = article_elem.find("./Language")
    if lang_elem is not None and lang_elem.text:
        info["language"] = lang_elem.text

    # Grants
    grants = _extract_grants(article_elem)
    if grants:
        info["grants"] = grants

    return info


def _get_element_text(elem: ET.Element) -> str:
    """Get all text from an element, including mixed content with child tags."""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        if child.text:
            parts.append(child.text)
        if child.tail:
            parts.append(child.tail)
    return "".join(parts).strip()


def _extract_abstract(article_elem: ET.Element) -> str:
    """Extract abstract, handling structured abstracts with labeled sections."""
    abstract_elem = article_elem.find("./Abstract")
    if abstract_elem is None:
        return ""

    parts = []
    for text_elem in abstract_elem.findall("./AbstractText"):
        label = text_elem.get("Label", "")
        text = _get_element_text(text_elem)
        if label:
            parts.append(f"{label}: {text}")
        else:
            parts.append(text)

    return " ".join(parts)


def _extract_authors(article_elem: ET.Element) -> list[dict[str, str]]:
    """Extract author information."""
    authors = []
    author_list = article_elem.find("./AuthorList")
    if author_list is None:
        return authors

    for author_elem in author_list.findall("./Author"):
        author: dict[str, str] = {}

        last_name = author_elem.find("./LastName")
        if last_name is not None and last_name.text:
            author["last_name"] = last_name.text

        fore_name = author_elem.find("./ForeName")
        if fore_name is not None and fore_name.text:
            author["first_name"] = fore_name.text

        initials = author_elem.find("./Initials")
        if initials is not None and initials.text:
            author["initials"] = initials.text

        affiliation = author_elem.find("./AffiliationInfo/Affiliation")
        if affiliation is not None and affiliation.text:
            author["affiliation"] = affiliation.text

        if "last_name" in author:
            authors.append(author)

    return authors


def _extract_publication_types(article_elem: ET.Element) -> list[dict[str, str]]:
    """Extract publication types."""
    pub_types = []
    pub_type_list = article_elem.find("./PublicationTypeList")
    if pub_type_list is None:
        return pub_types

    for pt_elem in pub_type_list.findall("./PublicationType"):
        if pt_elem.text:
            pub_types.append({
                "type": pt_elem.text,
                "ui": pt_elem.get("UI", ""),
            })

    return pub_types


def _extract_publication_date(article_elem: ET.Element) -> dict[str, Any]:
    """Extract publication date from JournalIssue or ArticleDate."""
    journal_issue = article_elem.find("./Journal/JournalIssue")
    if journal_issue is not None:
        pub_date_elem = journal_issue.find("./PubDate")
        if pub_date_elem is not None:
            date_info = _parse_date_element(pub_date_elem)
            if date_info:
                return date_info

    article_date = article_elem.find("./ArticleDate")
    if article_date is not None:
        return _parse_date_element(article_date)

    return {}


def _parse_date_element(date_elem: ET.Element) -> dict[str, Any]:
    """Parse a date element (PubDate or ArticleDate)."""
    info: dict[str, Any] = {}

    year_elem = date_elem.find("./Year")
    if year_elem is not None and year_elem.text:
        try:
            info["year"] = int(year_elem.text)
        except ValueError:
            info["year"] = year_elem.text

    month_elem = date_elem.find("./Month")
    if month_elem is not None and month_elem.text:
        month_text = month_elem.text
        try:
            month_num = int(month_text)
            if 1 <= month_num <= 12:
                info["month"] = month_num
        except ValueError:
            try:
                date_obj = datetime.strptime(month_text[:3], "%b")
                info["month"] = date_obj.month
            except ValueError:
                info["month"] = month_text

    day_elem = date_elem.find("./Day")
    if day_elem is not None and day_elem.text:
        try:
            info["day"] = int(day_elem.text)
        except ValueError:
            info["day"] = day_elem.text

    # Build ISO date string
    if "year" in info:
        iso = str(info["year"])
        if isinstance(info.get("month"), int):
            iso += f"-{info['month']:02d}"
            if isinstance(info.get("day"), int):
                iso += f"-{info['day']:02d}"
        info["iso_date"] = iso

    return info


def _extract_grants(article_elem: ET.Element) -> list[dict[str, str]]:
    """Extract grant information."""
    grants = []
    grant_list = article_elem.find("./GrantList")
    if grant_list is None:
        return grants

    for grant_elem in grant_list.findall("./Grant"):
        grant: dict[str, str] = {}

        grant_id = grant_elem.find("./GrantID")
        if grant_id is not None and grant_id.text:
            grant["id"] = grant_id.text

        agency = grant_elem.find("./Agency")
        if agency is not None and agency.text:
            grant["agency"] = agency.text

        country = grant_elem.find("./Country")
        if country is not None and country.text:
            grant["country"] = country.text

        acronym = grant_elem.find("./Acronym")
        if acronym is not None and acronym.text:
            grant["acronym"] = acronym.text

        if grant:
            grants.append(grant)

    return grants


def _extract_mesh_headings(medline: ET.Element) -> list[dict[str, Any]]:
    """Extract MeSH headings."""
    headings = []
    mesh_list = medline.find("./MeshHeadingList")
    if mesh_list is None:
        return headings

    for mesh_elem in mesh_list.findall("./MeshHeading"):
        heading: dict[str, Any] = {}

        descriptor = mesh_elem.find("./DescriptorName")
        if descriptor is not None and descriptor.text:
            heading["descriptor"] = descriptor.text
            heading["descriptor_ui"] = descriptor.get("UI", "")
            heading["descriptor_major"] = descriptor.get("MajorTopicYN", "N") == "Y"

        qualifiers = []
        for qualifier in mesh_elem.findall("./QualifierName"):
            if qualifier.text:
                qualifiers.append({
                    "name": qualifier.text,
                    "ui": qualifier.get("UI", ""),
                    "major": qualifier.get("MajorTopicYN", "N") == "Y",
                })
        if qualifiers:
            heading["qualifiers"] = qualifiers

        if "descriptor" in heading:
            headings.append(heading)

    return headings


def _extract_keywords(medline: ET.Element) -> list[dict[str, Any]]:
    """Extract keywords."""
    all_keywords = []
    for keyword_list in medline.findall("./KeywordList"):
        owner = keyword_list.get("Owner", "")
        for kw_elem in keyword_list.findall("./Keyword"):
            if kw_elem.text:
                all_keywords.append({
                    "keyword": kw_elem.text,
                    "major": kw_elem.get("MajorTopicYN", "N") == "Y",
                    "owner": owner,
                })
    return all_keywords


def _extract_journal_info(medline: ET.Element) -> dict[str, Any]:
    """Extract journal information from both MedlineJournalInfo and Article/Journal."""
    info: dict[str, Any] = {}

    # MedlineJournalInfo
    mj = medline.find("./MedlineJournalInfo")
    if mj is not None:
        ta = mj.find("./MedlineTA")
        if ta is not None and ta.text:
            info["title_abbreviated"] = ta.text
        nlm = mj.find("./NlmUniqueID")
        if nlm is not None and nlm.text:
            info["nlm_id"] = nlm.text
        issn_link = mj.find("./ISSNLinking")
        if issn_link is not None and issn_link.text:
            info["issn_linking"] = issn_link.text
        country = mj.find("./Country")
        if country is not None and country.text:
            info["country"] = country.text

    # Article/Journal
    article_elem = medline.find("./Article")
    if article_elem is not None:
        journal_elem = article_elem.find("./Journal")
        if journal_elem is not None:
            title = journal_elem.find("./Title")
            if title is not None and title.text:
                info["title"] = title.text
            iso_abbr = journal_elem.find("./ISOAbbreviation")
            if iso_abbr is not None and iso_abbr.text:
                info["iso_abbreviation"] = iso_abbr.text
            issn = journal_elem.find("./ISSN")
            if issn is not None and issn.text:
                info["issn"] = issn.text
                info["issn_type"] = issn.get("IssnType", "")

            ji = journal_elem.find("./JournalIssue")
            if ji is not None:
                vol = ji.find("./Volume")
                if vol is not None and vol.text:
                    info["volume"] = vol.text
                issue = ji.find("./Issue")
                if issue is not None and issue.text:
                    info["issue"] = issue.text
                info["cited_medium"] = ji.get("CitedMedium", "")

            pagination = article_elem.find("./Pagination/MedlinePgn")
            if pagination is not None and pagination.text:
                info["pagination"] = pagination.text

    return info


def _extract_pubmed_data(pubmed_data: ET.Element) -> dict[str, Any]:
    """Extract PubmedData (DOI, article IDs, publication status, history)."""
    info: dict[str, Any] = {}

    pub_status = pubmed_data.find("./PublicationStatus")
    if pub_status is not None and pub_status.text:
        info["publication_status"] = pub_status.text

    article_ids = []
    id_list = pubmed_data.find("./ArticleIdList")
    if id_list is not None:
        for id_elem in id_list.findall("./ArticleId"):
            if id_elem.text:
                id_type = id_elem.get("IdType", "")
                article_ids.append({"id": id_elem.text, "type": id_type})
                if id_type == "doi":
                    info["doi"] = id_elem.text
    if article_ids:
        info["article_ids"] = article_ids

    history = []
    hist_elem = pubmed_data.find("./History")
    if hist_elem is not None:
        for date_elem in hist_elem.findall("./PubMedPubDate"):
            status = date_elem.get("PubStatus", "")
            date_info = _parse_date_element(date_elem)
            if date_info and status:
                date_info["status"] = status
                history.append(date_info)
    if history:
        info["history"] = history

    return info


# ── Post-parse enrichments (migrated from S2_Prepare_and_ManualLabels.py) ──


def prepare_llm_input(article: dict[str, Any]) -> str:
    """Prepare a clean input string for LLM extraction.

    Args:
        article: Parsed article dict.

    Returns:
        Formatted string with title, abstract, and keywords.
    """
    title = article.get("title", "")
    abstract = article.get("abstract", "")

    llm_input = f"# Title:\n{title}\n\n# Abstract:\n{abstract}\n\n"

    # Add keywords
    combined_kw = _get_combined_keywords(article)
    if combined_kw:
        llm_input += f"# Keywords:\n{'; '.join(combined_kw)}\n\n"

    return llm_input


def compute_exclusion_flags(article: dict[str, Any]) -> tuple[bool, str]:
    """Determine if the article should be excluded based on publication type.

    Args:
        article: Parsed article dict.

    Returns:
        Tuple of (exclude_flag, exclude_reason).
    """
    pub_types = article.get("publication_types", [])
    for pt in pub_types:
        type_name = pt.get("type", "")
        if type_name == "Review":
            return True, "Review"
        if type_name == "Case Reports":
            return True, "Case Report"
        if type_name in ("Comment", "Letter"):
            return True, "Letter or Comment"
    return False, ""


def _get_combined_keywords(article: dict[str, Any]) -> list[str]:
    """Combine MeSH descriptors and keywords into a single list."""
    mesh = article.get("mesh_headings", [])
    keywords = article.get("keywords", [])

    combined = [m.get("descriptor", "") for m in mesh if m.get("descriptor")]
    combined += [k.get("keyword", "") for k in keywords if k.get("keyword")]
    return combined
