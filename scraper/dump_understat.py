import asyncio, json, ujson, re
from pathlib import Path
import aiohttp
from understat import Understat

# --- SETTINGS ---
LEAGUE = "EPL"   # Understat league code
SEASON = 2025    # 2025/26
DUMP_SHOTS = True
# ---------------

OUT = Path(__file__).resolve().parents[1] / "public"
OUT.mkdir(parents=True, exist_ok=True)

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

async def dump():
    async with aiohttp.ClientSession() as session:
        u = Understat(session)

        # 1) Get ALL results for the league/season (includes match id + teams + goals + datetime)
        results = await u.get_league_results(LEAGUE.lower(), SEASON)
        # results items look like:
        # {"id": "2179191", "h": {...}, "a": {...}, "goals": {"h": "2", "a": "1"}, "datetime": "2025-08-17 15:30:00", ...}
        # with "h" and "a" containing "title" (team names), etc.

        # 2) Derive team set and build per-team histories
        teams_titles = set()
        for r in results:
            if "h" in r and r["h"] and "title" in r["h"]:
                teams_titles.add(r["h"]["title"])
            if "a" in r and r["a"] and "title" in r["a"]:
                teams_titles.add(r["a"]["title"])

        teams_out = []
        for title in sorted(teams_titles):
            teams_out.append({
                "id": None,                # Understat Python API doesn't expose numeric team id here; not needed for your Sheet
                "title": title,
                "slug": slugify(title),
            })

        # 3) Write league teams list
        league_dir = OUT / "league" / LEAGUE
        league_dir.mkdir(parents=True, exist_ok=True)
        with open(league_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
            json.dump({"teams": teams_out}, f, ensure_ascii=False)

        # 4) For each team, build history from league results (guaranteed match IDs)
        #    history rows: {id, h_a, scored, conceded, date}
        by_team_slug = {t["slug"]: t["title"] for t in teams_out}

        for slug, title in by_team_slug.items():
            # map friendly aliases back to official titles, e.g., "man-city" -> "Manchester City"
            for alias, true_title in ALIASES.items():
                if slug == alias:
                    title = true_title
                    break

            history = []
            for r in results:
                # ensure we have fields
                mid = r.get("id")
                dt = r.get("datetime")
                h = r.get("h") or {}
                a = r.get("a") or {}
                h_title = h.get("title")
                a_title = a.get("title")
                goals = r.get("goals") or {}
                h_goals = goals.get("h")
                a_goals = goals.get("a")

                if title == h_title or title == a_title:
                    is_home = (title == h_title)
                    scored = int(h_goals) if is_home else int(a_goals)
                    conceded = int(a_goals) if is_home else int(h_goals)
                    history.append({
                        "id": int(mid) if mid is not None else None,
                        "h_a": "h" if is_home else "a",
                        "scored": scored,
                        "conceded": conceded,
                        "date": dt,
                    })

            # sort by date and write file
            history.sort(key=lambda x: x["date"] or "")
            team_dir = OUT / "team" / slug
            team_dir.mkdir(parents=True, exist_ok=True)
            with open(team_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
                ujson.dump({"history": history}, f, ensure_ascii=False)

        # 5) (Optional) dump shots for each match once
        if DUMP_SHOTS:
            match_dir = OUT / "match"
            match_dir.mkdir(parents=True, exist_ok=True)
            seen = set()
            for r in results:
                mid = r.get("id")
                if not mid or mid in seen:
                    continue
                seen.add(mid)
                shots_path = match_dir / f"{int(mid)}.json"
                if shots_path.exists():
                    continue
                shots = await u.get_match_shots(int(mid))  # { "h": [...], "a": [...] }
                with open(shots_path, "w", encoding="utf-8") as f:
                    ujson.dump(shots, f, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(dump())
