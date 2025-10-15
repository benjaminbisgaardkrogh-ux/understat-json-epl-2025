import asyncio, json, ujson, re
from pathlib import Path
import aiohttp
from understat import Understat
from collections import defaultdict

# --- SETTINGS ---
LEAGUE = "EPL"   # Understat league code
SEASON = 2025    # 2025/26
DUMP_SHOTS = True
# ---------------

OUT = Path(__file__).resolve().parents[1] / "public"
OUT.mkdir(parents=True, exist_ok=True)

# alias slug -> official title (used only to create extra alias files)
ALIASES = {
    "man-city": "Manchester City",
    "man-utd": "Manchester United",
    "man-united": "Manchester United",
    "spurs": "Tottenham",
    "wolves": "Wolverhampton",
    "west-ham": "West Ham United",
    "bmouth": "Bournemouth",
    "newcastle": "Newcastle United",
    "forest": "Nottingham Forest",
    "sheff-utd": "Sheffield United",
    "sheffield-utd": "Sheffield United",
    "palace": "Crystal Palace",
}

def slugify(s: str) -> str:
    s = s.lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

def alias_slugs_for_title(title: str):
    """Return alias slugs that should mirror this title (so both URLs work)."""
    official = slugify(title)
    out = []
    for alias_slug, alias_title in ALIASES.items():
        if alias_title == title and alias_slug != official:
            out.append(alias_slug)
    return out

async def dump():
    async with aiohttp.ClientSession() as session:
        u = Understat(session)

        # 1) Pull ALL results once (has match id + both teams + goals + datetime)
        results = await u.get_league_results(LEAGUE.lower(), SEASON)

        # 2) Build per-team histories directly from results (keyed by slug)
        histories = defaultdict(list)  # slug -> [rows]
        titles_set = set()

        for r in results:
            mid = r.get("id")
            dt = r.get("datetime")
            h = r.get("h") or {}
            a = r.get("a") or {}
            goals = r.get("goals") or {}
            h_title = h.get("title")
            a_title = a.get("title")
            h_goals = goals.get("h")
            a_goals = goals.get("a")

            if not (mid and dt and h_title and a_title and h_goals is not None and a_goals is not None):
                continue

            # normalize
            mid = int(mid)
            h_goals = int(h_goals)
            a_goals = int(a_goals)

            # record team titles for league list
            titles_set.add(h_title)
            titles_set.add(a_title)

            # home row
            h_slug = slugify(h_title)
            histories[h_slug].append({
                "id": mid, "h_a": "h",
                "scored": h_goals, "conceded": a_goals,
                "date": dt
            })

            # away row
            a_slug = slugify(a_title)
            histories[a_slug].append({
                "id": mid, "h_a": "a",
                "scored": a_goals, "conceded": h_goals,
                "date": dt
            })

        # 3) Sort histories and write team files for BOTH official and alias slugs
        for title in sorted(titles_set):
            official_slug = slugify(title)
            team_hist = histories.get(official_slug, [])
            team_hist.sort(key=lambda x: x["date"] or "")

            # write official
            team_dir = OUT / "team" / official_slug
            team_dir.mkdir(parents=True, exist_ok=True)
            with open(team_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
                ujson.dump({"history": team_hist}, f, ensure_ascii=False)

            # write aliases (duplicate the same JSON so both URLs work)
            for a_slug in alias_slugs_for_title(title):
                a_dir = OUT / "team" / a_slug
                a_dir.mkdir(parents=True, exist_ok=True)
                with open(a_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
                    ujson.dump({"history": team_hist}, f, ensure_ascii=False)

        # 4) League teams list from titles_set (id unknown here; keep None)
        teams_out = [{"id": None, "title": t, "slug": slugify(t)} for t in sorted(titles_set)]
        league_dir = OUT / "league" / LEAGUE
        league_dir.mkdir(parents=True, exist_ok=True)
        with open(league_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
            json.dump({"teams": teams_out}, f, ensure_ascii=False)

        # 5) Optional: dump per-match shots once
        if DUMP_SHOTS:
            match_dir = OUT / "match"
            match_dir.mkdir(parents=True, exist_ok=True)
            seen = set()
            for r in results:
                mid = r.get("id")
                if not mid:
                    continue
                mid = int(mid)
                if mid in seen:
                    continue
                seen.add(mid)
                shots_path = match_dir / f"{mid}.json"
                if shots_path.exists():
                    continue
                shots = await u.get_match_shots(mid)
                with open(shots_path, "w", encoding="utf-8") as f:
                    ujson.dump(shots, f, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(dump())
