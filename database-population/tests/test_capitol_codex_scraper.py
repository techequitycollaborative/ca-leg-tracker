"""Unit tests for the Capitol Codex staffer sheet scraper (no network)."""

import numpy as np
import pandas as pd
import pytest

import sources.capitol_codex_scraper as codex


def sample_sheet(district_prefix):
    """Five districts laid out like the live sheet: ID columns, then one column per issue area."""
    p = district_prefix
    return pd.DataFrame(
        {
            "District": [f"{p}01", f"{p}02", f"{p}03", f"{p}04", f"{p}05"],
            "Party": ["D", "R", "X", np.nan, "D"],
            "Member": ["Smith", "Jones", "ZZ-VACANT", "VACANT", np.nan],
            "Housing": ["Jane Doe", "John Roe", "Stale Staffer", "Old Aide", "By issue area"],
            "Approps": ["CMTE/Sam Lee", "Ann Bo & Cy Di", "Stale Staffer", "Old Aide", "By issue area"],
        }
    )


@pytest.fixture
def serve_sheets(monkeypatch):
    """Serve sample_sheet() for each chamber's sheet URL instead of downloading it."""
    sheets = {
        codex.build_sheet_url(codex.SHEET_LINKS["asm"]): sample_sheet("AD"),
        codex.build_sheet_url(codex.SHEET_LINKS["sen"]): sample_sheet("SD"),
    }
    monkeypatch.setattr(codex.pd, "read_csv", lambda url: sheets[url])
    return sheets


@pytest.mark.parametrize("chamber", ["asm", "sen"])
def test_drops_vacant_and_blank_member_rows(serve_sheets, chamber):
    df = codex.scrape_clean_sheet(chamber)

    # Districts 3-5 are "ZZ-VACANT", "VACANT", and blank
    assert df["district_number"].tolist() == [1, 2]
    # ID columns are dropped; district_number must stay last for extract_contacts
    assert list(df.columns) == ["Housing", "Approps", "district_number"]


def test_keeps_member_whose_name_contains_vacant(serve_sheets):
    asm_url = codex.build_sheet_url(codex.SHEET_LINKS["asm"])
    serve_sheets[asm_url].loc[0, "Member"] = "Vacanti"

    assert codex.scrape_clean_sheet("asm")["district_number"].tolist() == [1, 2]


def test_extracts_staffers_per_issue_area(serve_sheets):
    results = codex.extract_contacts("asm")

    assert set(results) == {"Housing", "Approps"}

    approps = results["Approps"].set_index("staffer_contact")
    # "CMTE/" prefix marks a committee staffer and is stripped from the name
    assert approps.loc["Sam Lee", "staffer_type"] == "committee"
    # "Ann Bo & Cy Di" is split into two staffers, each with a generated email
    assert approps.loc["Ann Bo", "generated_email"] == "ann.bo@asm.ca.gov"
    assert approps.loc["Cy Di", "generated_email"] == "cy.di@asm.ca.gov"
