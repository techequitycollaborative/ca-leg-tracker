import numpy as np
import pandas as pd
import pytest

import sources.capitol_codex_scraper as codex


def _sheet(prefix):
    # Mirrors the live sheet layout: ID columns, then one column per issue area
    return pd.DataFrame(
        {
            "District": [f"{prefix}01", f"{prefix}02", f"{prefix}03", f"{prefix}04", f"{prefix}05"],
            "Party": ["D", "R", "X", np.nan, "D"],
            "Member": ["Smith", "Jones", "ZZ-VACANT", "VACANT", np.nan],
            "Housing": ["Jane Doe", "John Roe", "Stale Staffer", "Old Aide", "By issue area"],
            "Approps": ["CMTE/Sam Lee", "Ann Bo & Cy Di", "Stale Staffer", "Old Aide", "By issue area"],
        }
    )


@pytest.fixture
def fake_sheet(monkeypatch):
    def fake_read_csv(url):
        return _sheet("AD" if "1302716632" in url else "SD")

    monkeypatch.setattr(codex.pd, "read_csv", fake_read_csv)


@pytest.mark.parametrize("chamber", ["asm", "sen"])
def test_scrape_clean_sheet_drops_vacant_and_blank_members(fake_sheet, chamber):
    df = codex.scrape_clean_sheet(chamber)

    assert df["district_number"].tolist() == [1, 2]
    # ID columns dropped; district_number stays last for extract_contacts
    assert list(df.columns) == ["Housing", "Approps", "district_number"]


def test_scrape_clean_sheet_keeps_member_containing_vacant_substring(monkeypatch):
    sheet = _sheet("AD")
    sheet.loc[0, "Member"] = "Vacanti"
    monkeypatch.setattr(codex.pd, "read_csv", lambda url: sheet)

    assert 1 in codex.scrape_clean_sheet("asm")["district_number"].tolist()


def test_extract_contacts_excludes_vacant_districts(fake_sheet):
    results = codex.extract_contacts("asm")

    assert set(results) == {"Housing", "Approps"}
    all_rows = pd.concat(results.values())
    assert set(all_rows["district_number"]) == {1, 2}
    assert "Stale Staffer" not in all_rows["staffer_contact"].tolist()
    assert "Old Aide" not in all_rows["staffer_contact"].tolist()

    approps = results["Approps"].set_index("staffer_contact")
    assert approps.loc["Sam Lee", "staffer_type"] == "committee"
    assert approps.loc["Ann Bo", "generated_email"] == "ann.bo@asm.ca.gov"
    assert approps.loc["Cy Di", "generated_email"] == "cy.di@asm.ca.gov"
