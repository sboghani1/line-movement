#!/usr/bin/env python3
"""
test_stage2_e2e.py — End-to-end test of the Python Stage 2 pipeline.

Connects to the real Google Sheet, initializes both resolvers, and runs
a handful of representative parsed_picks rows through finalize_picks_python()
to verify:
  1. Known teams resolve correctly (ESPN name, game, spread)
  2. Known cappers resolve correctly (canonical unique_capper_name)
  3. Unknown teams produce the "team_not_found" sentinel
  4. Unknown cappers produce the "capper_not_found" sentinel

Does NOT write anything to the sheet — read-only test.

Usage:
  .venv/bin/python3 test_stage2_e2e.py
"""

from dotenv import load_dotenv

from sheets_utils import GOOGLE_SHEET_ID, get_gspread_client
from team_resolver import TeamResolver
from capper_resolver import CapperResolver
from stage2_python import finalize_picks_python, TEAM_NOT_FOUND, CAPPER_NOT_FOUND

load_dotenv()


def main():
    print("Connecting to Google Sheets...")
    gc = get_gspread_client()
    ss = gc.open_by_key(GOOGLE_SHEET_ID)

    print("\nInitializing resolvers...")
    team_resolver = TeamResolver(ss)
    capper_resolver = CapperResolver(ss)

    # ── Test rows ────────────────────────────────────────────────────────────
    # Format: [date, capper, sport, pick, line, game, spread, result, ocr_text]
    # game/spread/result are blank — Stage 2 fills them.
    test_rows = [
        # 1. Known NBA team + known capper (should resolve)
        ["2026-03-23", "BEEZO WINS", "nba", "Brooklyn Nets", "-3.5", "", "", "", "BKN -3.5"],

        # 2. NBA abbreviation (alias) + known capper
        ["2026-03-23", "BEEZO WINS", "nba", "BKN", "ML", "", "", "", "BKN ML"],

        # 3. NHL team
        ["2026-03-23", "BEEZO WINS", "nhl", "Rangers", "-1.5", "", "", "", "Rangers -1.5"],

        # 4. CBB team (Duke plays on 2026-03-21)
        ["2026-03-21", "BEEZO WINS", "cbb", "Duke", "-5", "", "", "", "Duke -5"],

        # 5. Unknown team (should produce team_not_found)
        ["2026-03-23", "BEEZO WINS", "nba", "ZZZZZ Nonexistent", "ML", "", "", "", "ZZZZZ ML"],

        # 6. Unknown capper (should produce capper_not_found)
        ["2026-03-23", "TOTALLY_FAKE_CAPPER_XYZ", "nba", "Brooklyn Nets", "-3.5", "", "", "", "BKN -3.5"],

        # 7. Both unknown
        ["2026-03-23", "TOTALLY_FAKE_CAPPER_XYZ", "nba", "ZZZZZ Nonexistent", "ML", "", "", "", "ZZZZZ ML"],
    ]

    print(f"\nRunning Stage 2 on {len(test_rows)} test rows...\n")
    results = finalize_picks_python(team_resolver, capper_resolver, test_rows)

    # ── Report ───────────────────────────────────────────────────────────────
    headers = ["date", "capper", "sport", "pick", "line", "game", "spread", "result"]

    all_pass = True
    for i, (inp, out) in enumerate(zip(test_rows, results), start=1):
        print(f"{'─' * 70}")
        print(f"Test {i}:")
        print(f"  INPUT:  pick={inp[3]!r}  capper={inp[1]!r}  sport={inp[2]!r}  line={inp[4]!r}")
        print(f"  OUTPUT: pick={out[3]!r}  capper={out[1]!r}  game={out[5]!r}  spread={out[6]!r}")

        # Validate expectations
        if i == 5:  # Unknown team
            if out[5] == TEAM_NOT_FOUND:
                print(f"  ✅ PASS — team_not_found sentinel set correctly")
            else:
                print(f"  ❌ FAIL — expected game='{TEAM_NOT_FOUND}', got '{out[5]}'")
                all_pass = False
        elif i == 6:  # Unknown capper
            if out[1] == CAPPER_NOT_FOUND:
                print(f"  ✅ PASS — capper_not_found sentinel set correctly")
            else:
                print(f"  ❌ FAIL — expected capper='{CAPPER_NOT_FOUND}', got '{out[1]}'")
                all_pass = False
        elif i == 7:  # Both unknown
            t_ok = out[5] == TEAM_NOT_FOUND
            c_ok = out[1] == CAPPER_NOT_FOUND
            if t_ok and c_ok:
                print(f"  ✅ PASS — both sentinels set correctly")
            else:
                if not t_ok:
                    print(f"  ❌ FAIL — expected game='{TEAM_NOT_FOUND}', got '{out[5]}'")
                if not c_ok:
                    print(f"  ❌ FAIL — expected capper='{CAPPER_NOT_FOUND}', got '{out[1]}'")
                all_pass = False
        else:  # Known teams/cappers — should resolve
            if out[5] == TEAM_NOT_FOUND:
                print(f"  ❌ FAIL — team should have resolved but got team_not_found")
                all_pass = False
            elif not out[5]:
                print(f"  ❌ FAIL — game is empty (should have resolved)")
                all_pass = False
            else:
                print(f"  ✅ PASS — resolved successfully")

            if out[1] == CAPPER_NOT_FOUND:
                print(f"  ❌ FAIL — capper should have resolved but got capper_not_found")
                all_pass = False

    print(f"\n{'─' * 70}")
    if all_pass:
        print("🎉 ALL TESTS PASSED")
    else:
        print("⚠️  SOME TESTS FAILED — review output above")

    # ── Also test with real parsed_picks data from the sheet ─────────────
    print(f"\n{'═' * 70}")
    print("Testing with real parsed_picks data from sheet...")
    try:
        ws = ss.worksheet("parsed_picks")
        all_values = ws.get_all_values()
        if len(all_values) > 3:
            real_rows = all_values[3:]  # skip header rows
            real_rows = [r for r in real_rows if r and any(cell.strip() for cell in r)]
            if real_rows:
                print(f"Found {len(real_rows)} real parsed_picks rows")
                real_results = finalize_picks_python(team_resolver, capper_resolver, real_rows)

                not_found_teams = sum(1 for r in real_results if len(r) > 5 and r[5] == TEAM_NOT_FOUND)
                not_found_cappers = sum(1 for r in real_results if len(r) > 1 and r[1] == CAPPER_NOT_FOUND)
                resolved = len(real_results) - not_found_teams

                print(f"\nReal data results:")
                print(f"  Total rows:       {len(real_results)}")
                print(f"  Teams resolved:   {resolved}/{len(real_results)} ({resolved/len(real_results)*100:.1f}%)")
                print(f"  team_not_found:   {not_found_teams}")
                print(f"  capper_not_found: {not_found_cappers}")

                if not_found_teams:
                    print(f"\n  Unresolved teams (first 10):")
                    shown = 0
                    for inp, out in zip(real_rows, real_results):
                        if len(out) > 5 and out[5] == TEAM_NOT_FOUND:
                            print(f"    [{out[0]}] {out[2]:3s} | pick={inp[3]!r}")
                            shown += 1
                            if shown >= 10:
                                break

                if not_found_cappers:
                    print(f"\n  Unresolved cappers (first 10):")
                    shown = 0
                    for inp, out in zip(real_rows, real_results):
                        if len(out) > 1 and out[1] == CAPPER_NOT_FOUND:
                            print(f"    [{out[0]}] raw_capper={inp[1]!r}")
                            shown += 1
                            if shown >= 10:
                                break
            else:
                print("parsed_picks has no data rows")
        else:
            print("parsed_picks sheet is empty or has no data")
    except Exception as e:
        print(f"Could not test with real data: {e}")


if __name__ == "__main__":
    main()
