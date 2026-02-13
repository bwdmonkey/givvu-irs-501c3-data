"""Unit tests for the 990 XML parser.

Uses synthetic XML snippets to verify extraction of header, financial
summary, contact info, and Schedule M fields.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

from pipeline.parse_990 import (
    _safe_bool,
    _safe_date,
    _safe_int,
    parse_filing,
    parse_schedule_m,
)

# ── Helper: write a minimal 990 XML for testing ──────────────────────────

SAMPLE_990_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<Return xmlns="http://www.irs.gov/efile" returnVersion="2022v5.0">
  <ReturnHeader>
    <Filer>
      <EIN>123456789</EIN>
      <BusinessName>
        <BusinessNameLine1Txt>ACME FOOD BANK INC</BusinessNameLine1Txt>
      </BusinessName>
      <USAddress>
        <CityNm>SPRINGFIELD</CityNm>
        <StateAbbreviationCd>IL</StateAbbreviationCd>
        <ZIPCd>62701</ZIPCd>
      </USAddress>
      <PhoneNum>2175551234</PhoneNum>
    </Filer>
    <TaxYr>2022</TaxYr>
    <TaxPeriodBeginDt>2022-01-01</TaxPeriodBeginDt>
    <TaxPeriodEndDt>2022-12-31</TaxPeriodEndDt>
  </ReturnHeader>
  <ReturnData>
    <IRS990>
      <WebsiteAddressTxt>www.acmefoodbank.org</WebsiteAddressTxt>
      <PrincipalOfficerNm>Jane Doe</PrincipalOfficerNm>
      <OrganizationFoundedYr>1995</OrganizationFoundedYr>
      <ActivityOrMissionDesc>Feeding the hungry in Springfield</ActivityOrMissionDesc>
      <VotingMembersGoverningBodyCnt>12</VotingMembersGoverningBodyCnt>
      <VotingMembersIndependentCnt>10</VotingMembersIndependentCnt>
      <TotalEmployeeCnt>45</TotalEmployeeCnt>
      <TotalVolunteersCnt>200</TotalVolunteersCnt>
      <CYContributionsGrantsAmt>5000000</CYContributionsGrantsAmt>
      <CYTotalRevenueAmt>6000000</CYTotalRevenueAmt>
      <CYTotalExpensesAmt>5500000</CYTotalExpensesAmt>
      <TotalAssetsEOYAmt>2000000</TotalAssetsEOYAmt>
      <TotalLiabilitiesEOYAmt>500000</TotalLiabilitiesEOYAmt>
      <NetAssetsOrFundBalancesEOYAmt>1500000</NetAssetsOrFundBalancesEOYAmt>
      <NoncashContributionsInd>true</NoncashContributionsInd>
    </IRS990>
    <IRS990ScheduleM>
      <FoodInventoryGrp>
        <NoncashCheckboxInd>X</NoncashCheckboxInd>
        <NoncashContributionsCnt>350</NoncashContributionsCnt>
        <NoncashContributionsAmt>750000</NoncashContributionsAmt>
        <MethodOfDeterminingAmt>Cost</MethodOfDeterminingAmt>
      </FoodInventoryGrp>
      <ClothingAndHouseholdGoodsGrp>
        <NoncashCheckboxInd>X</NoncashCheckboxInd>
        <NoncashContributionsCnt>120</NoncashContributionsCnt>
        <NoncashContributionsAmt>50000</NoncashContributionsAmt>
        <MethodOfDeterminingAmt>FMV</MethodOfDeterminingAmt>
      </ClothingAndHouseholdGoodsGrp>
      <ReviewProcessUnusualNCGiftsInd>true</ReviewProcessUnusualNCGiftsInd>
      <ThirdPartiesUsedInd>false</ThirdPartiesUsedInd>
    </IRS990ScheduleM>
  </ReturnData>
</Return>
"""


def _write_sample_xml() -> Path:
    """Write the sample XML to a temp file and return its path."""
    tmp = tempfile.NamedTemporaryFile(suffix=".xml", delete=False, mode="w")
    tmp.write(SAMPLE_990_XML)
    tmp.close()
    return Path(tmp.name)


# ── Tests: utility functions ──────────────────────────────────────────────


class TestSafeConversions:
    def test_safe_int_valid(self) -> None:
        assert _safe_int("123") == 123
        assert _safe_int(" 456 ") == 456
        assert _safe_int("1,000") == 1000

    def test_safe_int_none(self) -> None:
        assert _safe_int(None) is None
        assert _safe_int("") is None
        assert _safe_int("abc") is None

    def test_safe_int_float_string(self) -> None:
        assert _safe_int("123.0") == 123

    def test_safe_bool_true(self) -> None:
        assert _safe_bool("true") is True
        assert _safe_bool("1") is True
        assert _safe_bool("X") is True
        assert _safe_bool("YES") is True

    def test_safe_bool_false(self) -> None:
        assert _safe_bool("false") is False
        assert _safe_bool("0") is False
        assert _safe_bool("NO") is False

    def test_safe_bool_none(self) -> None:
        assert _safe_bool(None) is None

    def test_safe_date_iso(self) -> None:
        assert _safe_date("2022-01-01") == "2022-01-01"

    def test_safe_date_compact(self) -> None:
        assert _safe_date("20220101") == "2022-01-01"

    def test_safe_date_none(self) -> None:
        assert _safe_date(None) is None
        assert _safe_date("") is None


# ── Tests: filing parser ──────────────────────────────────────────────────


class TestParseFiling:
    def setup_method(self) -> None:
        self.xml_path = _write_sample_xml()

    def test_basic_fields(self) -> None:
        filing = parse_filing(self.xml_path, "TEST_OBJ_001")
        assert filing is not None
        assert filing["object_id"] == "TEST_OBJ_001"
        assert filing["form_type"] == "990"

    def test_ein_extracted(self) -> None:
        filing = parse_filing(self.xml_path, "TEST_OBJ_001")
        assert filing is not None
        # EIN should be extracted, possibly padded
        ein = filing.get("ein")
        assert ein is not None
        assert "123456789" in ein

    def test_has_schedule_m(self) -> None:
        filing = parse_filing(self.xml_path, "TEST_OBJ_001")
        assert filing is not None
        assert filing["has_schedule_m"] is True


# ── Tests: Schedule M parser ─────────────────────────────────────────────


class TestParseScheduleM:
    def setup_method(self) -> None:
        self.xml_path = _write_sample_xml()

    def test_schedule_m_parsed(self) -> None:
        result = parse_schedule_m(self.xml_path, "TEST_OBJ_001", "123456789", 2022)
        assert result is not None
        assert result["object_id"] == "TEST_OBJ_001"
        assert result["ein"] == "123456789"
        assert result["tax_year"] == 2022

    def test_food_inventory(self) -> None:
        result = parse_schedule_m(self.xml_path, "TEST_OBJ_001", "123456789", 2022)
        assert result is not None
        assert result["food_inventory_x"] is True
        assert result["food_inventory_count"] == 350
        assert result["food_inventory_amount"] == 750000
        assert result["food_inventory_method"] == "Cost"

    def test_clothing_household(self) -> None:
        result = parse_schedule_m(self.xml_path, "TEST_OBJ_001", "123456789", 2022)
        assert result is not None
        assert result["clothing_household_x"] is True
        assert result["clothing_household_count"] == 120
        assert result["clothing_household_amount"] == 50000

    def test_empty_property_types(self) -> None:
        result = parse_schedule_m(self.xml_path, "TEST_OBJ_001", "123456789", 2022)
        assert result is not None
        # Art should not have been reported
        assert result["art_works_x"] is None
        assert result["art_works_count"] is None

    def test_summary_questions(self) -> None:
        result = parse_schedule_m(self.xml_path, "TEST_OBJ_001", "123456789", 2022)
        assert result is not None
        assert result["gift_acceptance_policy"] is True
        assert result["uses_third_parties"] is False

    def test_no_schedule_m_returns_none(self) -> None:
        """An XML without Schedule M should return None."""
        minimal = """\
<?xml version="1.0" encoding="utf-8"?>
<Return xmlns="http://www.irs.gov/efile">
  <ReturnData>
    <IRS990>
      <TotalEmployeeCnt>10</TotalEmployeeCnt>
    </IRS990>
  </ReturnData>
</Return>"""
        tmp = tempfile.NamedTemporaryFile(suffix=".xml", delete=False, mode="w")
        tmp.write(minimal)
        tmp.close()
        result = parse_schedule_m(Path(tmp.name), "OBJ_002", "999999999", 2023)
        assert result is None
