#!/usr/bin/env python3
"""Load the Master Concordance File and build xpath lookup maps.

The concordance maps IRS 990 XML xpaths to standardised variable names.
Because the IRS has changed XSD schemas across versions, a single variable
may correspond to multiple xpaths.  This module builds *reverse* maps:
for each of our target variables, collect all known xpaths so the parser
can try them in order.

If the concordance CSV is not already cached locally it is downloaded from
the Nonprofit Open Data Collective GitHub repository.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass, field
from pathlib import Path

import requests

from pipeline.config import CONCORDANCE_CSV_URL, CONCORDANCE_DIR

logger = logging.getLogger(__name__)

CONCORDANCE_LOCAL = CONCORDANCE_DIR / "concordance.csv"


# ── Download ──────────────────────────────────────────────────────────────


def download_concordance(force: bool = False) -> Path:
    """Download the master concordance CSV if not already cached."""
    if CONCORDANCE_LOCAL.exists() and not force:
        logger.debug("Concordance already cached at %s", CONCORDANCE_LOCAL)
        return CONCORDANCE_LOCAL

    logger.info("Downloading master concordance from GitHub …")
    resp = requests.get(CONCORDANCE_CSV_URL, timeout=120)
    resp.raise_for_status()
    CONCORDANCE_LOCAL.write_bytes(resp.content)
    logger.info("Concordance saved (%d KB)", len(resp.content) // 1024)
    return CONCORDANCE_LOCAL


# ── Mapping structures ────────────────────────────────────────────────────


@dataclass
class VariableInfo:
    """Metadata about a single concordance variable."""

    variable_name: str
    description: str = ""
    xpaths: list[str] = field(default_factory=list)


def load_concordance() -> dict[str, VariableInfo]:
    """Parse the concordance CSV into a dict keyed by variable_name.

    Each value is a ``VariableInfo`` with all known xpaths for that variable.
    """
    path = download_concordance()
    variables: dict[str, VariableInfo] = {}

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vname = (row.get("variable_name") or "").strip()
            xpath = (row.get("xpath") or "").strip()
            desc = (row.get("description") or "").strip()
            if not vname or not xpath:
                continue

            if vname not in variables:
                variables[vname] = VariableInfo(variable_name=vname, description=desc)
            variables[vname].xpaths.append(xpath)

    logger.info("Loaded %d unique variables from concordance.", len(variables))
    return variables


# ── Pre-built xpath sets for each target field ────────────────────────────
# We define a mapping from *our* output column names to concordance
# variable names.  The ``build_xpath_map`` function then resolves these
# into lists of xpaths that the XML parser can iterate.


# Header / identity fields (Part 00)
HEADER_VAR_MAP: dict[str, str] = {
    "org_name":                 "F9_00_ORG_NAME_L1",
    "org_city":                 "F9_00_ORG_ADDR_CITY",
    "org_state":                "F9_00_ORG_ADDR_STATE",
    "org_zip":                  "F9_00_ORG_ADDR_ZIP",
    "org_phone":                "F9_00_ORG_PHONE",
    "website":                  "F9_00_ORG_WEBSITE",
    "principal_officer_name":   "F9_00_PRIN_OFF_NAME_PERS",
    "ein":                      "F9_00_ORG_EIN",
    "tax_year":                 "F9_00_TAX_YEAR",
    "tax_period_begin":         "F9_00_TAX_PERIOD_BEGIN_DATE",
    "tax_period_end":           "F9_00_TAX_PERIOD_END_DATE",
    "year_formation":           "F9_00_YEAR_FORMATION",
}

# Signature block (Part 02)
SIGNATURE_VAR_MAP: dict[str, str] = {
    "signing_officer_name":     "F9_02_SIGNING_OFF_NAME",
    "signing_officer_title":    "F9_02_SIGNING_OFF_TITLE",
    "signing_officer_phone":    "F9_02_SIGNING_OFF_PHONE",
}

# Part I – Summary financial fields
SUMMARY_VAR_MAP: dict[str, str] = {
    "mission":                           "F9_03_ORG_MISSION_PURPOSE",
    "num_voting_members":                "F9_01_ACT_GVRN_NUM_VOTE_MEMB",
    "num_voting_members_independent":    "F9_01_ACT_GVRN_NUM_VOTE_MEMB_IND",
    "num_employees":                     "F9_01_ACT_GVRN_EMPL_TOT",
    "num_volunteers":                    "F9_01_ACT_GVRN_VOL_TOT",
    "contributions_grants_cy":           "F9_01_REV_CONTR_TOT_CY",
    "program_service_revenue_cy":        "F9_01_REV_PROG_TOT_CY",
    "investment_income_cy":              "F9_01_REV_INVEST_TOT_CY",
    "other_revenue_cy":                  "F9_01_REV_OTH_CY",
    "total_revenue_cy":                  "F9_01_REV_TOT_CY",
    "total_revenue_py":                  "F9_01_REV_TOT_PY",
    "grants_similar_cy":                 "F9_01_EXP_GRANT_SIMILAR_CY",
    "salaries_cy":                       "F9_01_EXP_SAL_ETC_CY",
    "total_expenses_cy":                 "F9_01_EXP_TOT_CY",
    "total_expenses_py":                 "F9_01_EXP_TOT_PY",
    "revenue_less_expenses_cy":          "F9_01_EXP_REV_LESS_EXP_CY",
    "total_assets_boy":                  "F9_01_NAFB_ASSET_TOT_BOY",
    "total_assets_eoy":                  "F9_01_NAFB_ASSET_TOT_EOY",
    "total_liabilities_boy":             "F9_01_NAFB_LIAB_TOT_BOY",
    "total_liabilities_eoy":             "F9_01_NAFB_LIAB_TOT_EOY",
    "net_assets_boy":                    "F9_01_NAFB_TOT_BOY",
    "net_assets_eoy":                    "F9_01_NAFB_TOT_EOY",
}


def build_xpath_map(
    concordance: dict[str, VariableInfo],
    var_map: dict[str, str],
) -> dict[str, list[str]]:
    """For each output column, resolve the concordance variable to a list of xpaths.

    Returns a dict: output_column_name → [xpath1, xpath2, ...].
    Missing variables are logged as warnings and mapped to an empty list.
    """
    result: dict[str, list[str]] = {}
    for col, var_name in var_map.items():
        info = concordance.get(var_name)
        if info:
            result[col] = info.xpaths
        else:
            logger.warning("Concordance variable %s not found (col=%s)", var_name, col)
            result[col] = []
    return result


# ── Convenience: pre-built maps for the parser ────────────────────────────


def get_all_xpath_maps() -> dict[str, dict[str, list[str]]]:
    """Return all xpath maps needed by the 990 XML parser.

    Returns a dict with keys ``header``, ``signature``, ``summary``.
    """
    concordance = load_concordance()
    return {
        "header": build_xpath_map(concordance, HEADER_VAR_MAP),
        "signature": build_xpath_map(concordance, SIGNATURE_VAR_MAP),
        "summary": build_xpath_map(concordance, SUMMARY_VAR_MAP),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    maps = get_all_xpath_maps()
    for section, xpath_map in maps.items():
        print(f"\n── {section} ({len(xpath_map)} fields) ──")
        for col, xpaths in xpath_map.items():
            print(f"  {col}: {len(xpaths)} xpaths")
