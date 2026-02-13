#!/usr/bin/env python3
"""Parse 990 XML e-files and extract header, financial summary, and Schedule M.

Reads XML files from ``data/xml/`` and writes two JSONL files:
  * ``data/parsed/filings.jsonl``   — header + Part I summary + contact info
  * ``data/parsed/schedule_m.jsonl`` — Schedule M noncash contribution details

The parser uses the Master Concordance File (via ``concordance.py``) to
resolve variable names into xpath expressions.  For Schedule M, which has
a highly structured fixed layout (28 property-type rows), we use both
concordance xpaths and direct element-name matching as a fallback.
"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
from pathlib import Path

from lxml import etree

from pipeline.concordance import get_all_xpath_maps
from pipeline.config import (
    PARSED_DIR,
    SCHEDULE_M_PROPERTY_TYPES,
    XML_DIR,
)

logger = logging.getLogger(__name__)

# Common IRS 990 XML namespaces (varies by version)
_NS_PREFIXES = [
    "",  # Try without namespace first
    "{http://www.irs.gov/efile}",
]


# ── Helpers ───────────────────────────────────────────────────────────────


def _find_text(root: etree._Element, xpaths: list[str]) -> str | None:
    """Try a list of xpaths and return the first matching text value."""
    for xpath in xpaths:
        try:
            elems = root.xpath(xpath)
            if elems:
                if isinstance(elems[0], etree._Element):
                    text = elems[0].text
                else:
                    text = str(elems[0])
                if text and text.strip():
                    return text.strip()
        except Exception:
            pass
    return None


def _find_element(root: etree._Element, local_names: list[str]) -> etree._Element | None:
    """Find the first element matching any of the given local names (ignoring namespace)."""
    for name in local_names:
        for prefix in _NS_PREFIXES:
            elems = root.findall(f".//{prefix}{name}")
            if elems:
                return elems[0]
    return None


def _el_text(parent: etree._Element | None, local_names: list[str]) -> str | None:
    """Get text from a child element matching any of the given local names."""
    if parent is None:
        return None
    for name in local_names:
        for prefix in _NS_PREFIXES:
            el = parent.find(f".//{prefix}{name}")
            if el is not None and el.text:
                return el.text.strip()
    return None


def _safe_int(val: str | None) -> int | None:
    if val is None:
        return None
    val = val.strip().replace(",", "")
    try:
        return int(val)
    except (ValueError, TypeError):
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None


def _safe_bool(val: str | None) -> bool | None:
    if val is None:
        return None
    val = val.strip().upper()
    if val in ("1", "TRUE", "X", "YES", "Y"):
        return True
    if val in ("0", "FALSE", "NO", "N", ""):
        return False
    return None


def _safe_date(val: str | None) -> str | None:
    """Normalise date strings to YYYY-MM-DD for BigQuery."""
    if not val:
        return None
    val = val.strip()
    if len(val) == 10 and "-" in val:
        return val  # Already YYYY-MM-DD
    if len(val) == 8 and val.isdigit():
        return f"{val[:4]}-{val[4:6]}-{val[6:8]}"
    return val


# ── Filing parser ─────────────────────────────────────────────────────────

# The xpath maps are loaded once when the module is first imported.
_XPATH_MAPS: dict[str, dict[str, list[str]]] | None = None


def _get_xpath_maps() -> dict[str, dict[str, list[str]]]:
    global _XPATH_MAPS
    if _XPATH_MAPS is None:
        _XPATH_MAPS = get_all_xpath_maps()
    return _XPATH_MAPS


def _extract_field(root: etree._Element, xpaths: list[str]) -> str | None:
    """Try concordance xpaths, then fall back to local-name matching."""
    # Try concordance xpaths
    val = _find_text(root, xpaths)
    if val:
        return val

    # Fallback: try the last part of each xpath as a local element name
    for xpath in xpaths:
        parts = xpath.rstrip("/").split("/")
        if parts:
            local = parts[-1]
            for prefix in _NS_PREFIXES:
                elems = root.findall(f".//{prefix}{local}")
                if elems and elems[0].text:
                    return elems[0].text.strip()
    return None


def parse_filing(xml_path: Path, object_id: str) -> dict | None:
    """Parse a single 990 XML file and return a filing dict (or None on error)."""
    try:
        tree = etree.parse(str(xml_path))
    except Exception:
        logger.debug("Failed to parse XML: %s", xml_path, exc_info=True)
        return None

    root = tree.getroot()
    maps = _get_xpath_maps()

    # Find the Return/ReturnData container
    return_data = _find_element(root, ["ReturnData"])
    search_root = return_data if return_data is not None else root

    filing: dict = {"object_id": object_id, "form_type": "990"}

    # ── Header fields ──
    for col, xpaths in maps["header"].items():
        val = _extract_field(search_root, xpaths)
        if col == "tax_year":
            filing[col] = _safe_int(val)
        elif col in ("tax_period_begin", "tax_period_end"):
            filing[col] = _safe_date(val)
        elif col == "year_formation":
            filing[col] = _safe_int(val)
        else:
            filing[col] = val

    # Fall back: extract EIN from the Return header if not found
    if not filing.get("ein"):
        ein_el = _find_element(root, ["Filer/EIN", "EIN"])
        if ein_el is not None and ein_el.text:
            filing["ein"] = ein_el.text.strip().zfill(9)

    # ── Signature / contact fields ──
    for col, xpaths in maps["signature"].items():
        filing[col] = _extract_field(search_root, xpaths)

    # ── Summary financial fields (Part I) ──
    _INT_SUMMARY = {
        "num_voting_members", "num_voting_members_independent",
        "num_employees", "num_volunteers",
        "contributions_grants_cy", "program_service_revenue_cy",
        "investment_income_cy", "other_revenue_cy",
        "total_revenue_cy", "total_revenue_py",
        "grants_similar_cy", "salaries_cy",
        "total_expenses_cy", "total_expenses_py",
        "revenue_less_expenses_cy",
        "total_assets_boy", "total_assets_eoy",
        "total_liabilities_boy", "total_liabilities_eoy",
        "net_assets_boy", "net_assets_eoy",
    }
    for col, xpaths in maps["summary"].items():
        val = _extract_field(search_root, xpaths)
        if col in _INT_SUMMARY:
            filing[col] = _safe_int(val)
        else:
            filing[col] = val

    # ── Noncash total (Part VIII Line 1g) ──
    noncash_names = [
        "NoncashContributionsAmt",
        "NoncashContributions",
        "AllOtherContributionsAmt",
    ]
    noncash_val = _el_text(search_root, noncash_names)
    filing["noncash_contributions_total"] = _safe_int(noncash_val)

    # ── Has Schedule M? (Part IV lines 29/30) ──
    sched_m_names = [
        "NoncashContributionsInd",
        "MoreThan25KNoncashInd",
        "ArtHistTreasuresContribInd",
        "MoreThan25000",
        "ArtHistTreasuresContrib",
    ]
    sched_m_val = None
    for name in sched_m_names:
        v = _el_text(search_root, [name])
        if v:
            sched_m_val = v
            break
    filing["has_schedule_m"] = _safe_bool(sched_m_val)

    return filing


# ── Schedule M parser ─────────────────────────────────────────────────────

# Mapping from our prefix → IRS XML element name patterns for each property
# type line on Schedule M.  The IRS uses various naming conventions.
_SCHED_M_LINE_NAMES: dict[str, list[str]] = {
    "art_works":                  ["ArtWorksOfArt", "ArtWorksOfArtGrp"],
    "art_historical":             ["ArtHistoricalTreasures", "ArtHistoricalTreasuresGrp"],
    "art_fractional":             ["ArtFractionalInterests", "ArtFractionalInterestsGrp"],
    "books_publications":         ["BooksAndPublications", "BooksAndPublicationsGrp"],
    "clothing_household":         ["ClothingAndHouseholdGoods", "ClothingAndHouseholdGoodsGrp"],
    "cars_vehicles":              ["CarsAndOtherVehicles", "CarsAndOtherVehiclesGrp"],
    "boats_planes":               ["BoatsAndPlanes", "BoatsAndPlanesGrp"],
    "intellectual_property":      ["IntellectualProperty", "IntellectualPropertyGrp"],
    "securities_publicly_traded": ["SecuritiesPubliclyTraded", "SecuritiesPubliclyTradedGrp"],
    "securities_closely_held":    ["SecuritiesCloselyHeldStock", "SecuritiesCloselyHeldStockGrp"],
    "securities_partnership":     ["SecuritiesPartnership", "SecuritiesPartnershipGrp",
                                   "SecPrtnrshpTrustInterests", "SecPrtnrshpTrustInterestsGrp"],
    "securities_misc":            ["SecuritiesMiscellaneous", "SecuritiesMiscellaneousGrp"],
    "conservation_historic":      ["QualifiedContribHistStruct", "QualifiedContribHistStructGrp"],
    "conservation_other":         ["QualifiedContribOther", "QualifiedContribOtherGrp"],
    "real_estate_residential":    ["RealEstateResidential", "RealEstateResidentialGrp"],
    "real_estate_commercial":     ["RealEstateCommercial", "RealEstateCommercialGrp"],
    "real_estate_other":          ["RealEstateOther", "RealEstateOtherGrp"],
    "collectibles":               ["Collectibles", "CollectiblesGrp"],
    "food_inventory":             ["FoodInventory", "FoodInventoryGrp"],
    "drugs_medical":              ["DrugsAndMedicalSupplies", "DrugsAndMedicalSuppliesGrp"],
    "taxidermy":                  ["Taxidermy", "TaxidermyGrp"],
    "historical_artifacts":       ["HistoricalArtifacts", "HistoricalArtifactsGrp"],
    "scientific_specimens":       ["ScientificSpecimens", "ScientificSpecimensGrp"],
    "archaeological_artifacts":   ["ArcheologicalArtifacts", "ArcheologicalArtifactsGrp"],
    "other_1":                    ["OtherNoncashContri25", "OtherNoncashContriTable25Grp"],
    "other_2":                    ["OtherNoncashContri26", "OtherNoncashContriTable26Grp"],
    "other_3":                    ["OtherNoncashContri27", "OtherNoncashContriTable27Grp"],
    "other_4":                    ["OtherNoncashContri28", "OtherNoncashContriTable28Grp"],
}

# Child element names for checkbox / count / amount / method within each
# property-type group element.
_CHECKBOX_NAMES = [
    "NoncashCheckboxInd", "NonCashCheckbox",
    "ContributionCheckInd", "Checkbox",
]
_COUNT_NAMES = [
    "NoncashContributionsCnt", "NoncashContributions",
    "ContributionsItemsCnt", "NumberOfContributions",
]
_AMOUNT_NAMES = [
    "NoncashContributionsAmt", "NoncashContributions",
    "FairMarketValueAmt", "FMVReportedAmt",
    "NoncashContributionAmt",
]
_METHOD_NAMES = [
    "MethodOfDeterminingAmt", "MethodOfDetermination",
    "MethodOfDeterminationDesc", "NoncashContributionMethod",
]
_DESC_NAMES = [
    "Desc", "Description", "TypeDesc",
]


def parse_schedule_m(xml_path: Path, object_id: str, ein: str | None, tax_year: int | None) -> dict | None:
    """Parse Schedule M from a 990 XML and return a flat dict (or None)."""
    try:
        tree = etree.parse(str(xml_path))
    except Exception:
        return None

    root = tree.getroot()

    # Find the Schedule M container
    sched_m = None
    for name in ["IRS990ScheduleM", "ScheduleM"]:
        sched_m = _find_element(root, [name])
        if sched_m is not None:
            break

    if sched_m is None:
        return None  # No Schedule M in this filing

    record: dict = {
        "object_id": object_id,
        "ein": ein or "",
        "tax_year": tax_year,
    }

    # ── Property types (lines 1-28) ──
    for _line, prefix, _desc in SCHEDULE_M_PROPERTY_TYPES:
        line_names = _SCHED_M_LINE_NAMES.get(prefix, [])
        grp = None
        for ln in line_names:
            grp = _find_element(sched_m, [ln])
            if grp is not None:
                break

        if grp is not None:
            record[f"{prefix}_x"] = _safe_bool(
                _el_text(grp, _CHECKBOX_NAMES)
            )
            record[f"{prefix}_count"] = _safe_int(
                _el_text(grp, _COUNT_NAMES)
            )
            record[f"{prefix}_amount"] = _safe_int(
                _el_text(grp, _AMOUNT_NAMES)
            )
            record[f"{prefix}_method"] = _el_text(grp, _METHOD_NAMES)

            if prefix.startswith("other_"):
                record[f"{prefix}_desc"] = _el_text(grp, _DESC_NAMES)
        else:
            record[f"{prefix}_x"] = None
            record[f"{prefix}_count"] = None
            record[f"{prefix}_amount"] = None
            record[f"{prefix}_method"] = None
            if prefix.startswith("other_"):
                record[f"{prefix}_desc"] = None

    # ── Summary questions (lines 29-32) ──
    record["num_forms_8283"] = _safe_int(
        _el_text(sched_m, ["NumberOf8283Received", "NumberOf8283ReceivedCnt", "Form8283ReceivedCnt"])
    )
    record["hold_3_years_required"] = _safe_bool(
        _el_text(sched_m, [
            "AnyPropertyThatMustBeHeldInd", "AnyPropertyThatMustBeHeld",
            "PropertyMustBeHeldInd",
        ])
    )
    record["gift_acceptance_policy"] = _safe_bool(
        _el_text(sched_m, [
            "ReviewProcessUnusualNCGiftsInd", "ReviewProcessUnusualNCGifts",
            "GiftAcceptancePolicyInd",
        ])
    )
    record["uses_third_parties"] = _safe_bool(
        _el_text(sched_m, [
            "ThirdPartiesUsedInd", "ThirdPartiesUsed",
            "HireOrUseThirdPartiesInd",
        ])
    )

    return record


# ── Batch parser (multiprocessing) ────────────────────────────────────────


def _parse_one(args: tuple[Path, str]) -> tuple[dict | None, dict | None]:
    """Worker function for multiprocessing: parse one XML into filing + schedule_m."""
    xml_path, object_id = args
    filing = parse_filing(xml_path, object_id)
    sched_m = None
    if filing:
        sched_m = parse_schedule_m(
            xml_path, object_id,
            ein=filing.get("ein"),
            tax_year=filing.get("tax_year"),
        )
    return filing, sched_m


def parse_all_xmls(
    force: bool = False,
    num_workers: int | None = None,
) -> tuple[Path, Path]:
    """Parse all downloaded XMLs and write filings.jsonl + schedule_m.jsonl.

    Returns (filings_path, schedule_m_path).
    """
    filings_path = PARSED_DIR / "filings.jsonl"
    sched_m_path = PARSED_DIR / "schedule_m.jsonl"

    if filings_path.exists() and sched_m_path.exists() and not force:
        logger.info("Parsed files already exist. Use force=True to re-parse.")
        return filings_path, sched_m_path

    # Gather XML files
    xml_files = sorted(XML_DIR.glob("*.xml"))
    if not xml_files:
        logger.warning("No XML files found in %s", XML_DIR)
        return filings_path, sched_m_path

    logger.info("Parsing %d XML files ...", len(xml_files))

    # Build work items: (path, object_id)
    work = []
    for p in xml_files:
        oid = p.stem  # e.g., "201541349349307794"
        # Remove trailing _public if present
        if oid.endswith("_public"):
            oid = oid[:-7]
        work.append((p, oid))

    if num_workers is None:
        num_workers = max(1, mp.cpu_count() - 1)

    filing_count = 0
    sched_m_count = 0

    with (
        open(filings_path, "w", encoding="utf-8") as f_filings,
        open(sched_m_path, "w", encoding="utf-8") as f_sched,
    ):
        # Use multiprocessing for CPU-bound XML parsing
        with mp.Pool(processes=num_workers) as pool:
            from tqdm import tqdm
            for filing, sched_m in tqdm(
                pool.imap_unordered(_parse_one, work, chunksize=100),
                total=len(work),
                desc="Parsing XMLs",
            ):
                if filing:
                    f_filings.write(json.dumps(filing, ensure_ascii=False, default=str) + "\n")
                    filing_count += 1
                if sched_m:
                    f_sched.write(json.dumps(sched_m, ensure_ascii=False, default=str) + "\n")
                    sched_m_count += 1

    logger.info(
        "Parsing complete: %d filings, %d with Schedule M",
        filing_count, sched_m_count,
    )
    return filings_path, sched_m_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parse_all_xmls(force=True)
