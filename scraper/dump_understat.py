import asyncio, json, ujson, re
from pathlib import Path
import aiohttp
from understat import Understat

# --- SETTINGS ---
LEAGUE = "EPL"   # Understat league code
SEASON = 2025    # 2025/26
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

        # 1) League teams (id, title, etc.)
        teams = await u.get_teams(LEAGUE.lower(), SEASON)

        teams_out = []
        id_by_title = {}
        for t in teams:
            title = t["title"]
            tid = int(t["id"])
            id_by_title[title] = tid
            teams_out.append({
                "id": str(tid),
                "title": title,
                "slug": slugify(title),
            })

        league_dir = OUT / "league" / LEAGUE
        league_dir.mkdir(parents=True, exist_ok=True)
        with open(league_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
            json.dump({ "teams": teams_out }, f, ensure_ascii=False)

        # 2) Each team: dump history with real match IDs + goals
        for t in teams_out:
            # map friendly slug to official title if needed
            title = None
            for alias, true_title in ALIASES.items():
                if t["slug"] == alias:
                    title = true_title
                    break
            if title is None:
                title = next((k for k in id_by_title if slugify(k) == t["slug"]), t["title"])

            team_id = id_by_title.get(title)
            if not team_id:
                for k, v in id_by_title.items():
                    if slugify(k) == t["slug"]:
                        team_id = v
                        break

            matches = await u.get_team_matches(team_id, SEASON)

            history = []
            for m in matches:
                is_home = bool(m.get("is_home"))
                scored = m.get("goals")
                conceded = m.get("goals_opp")
                mid = m.get("id")
                dt = m.get("datetime") or m.get("date")

                history.append({
                    "id": int(mid) if mid is not None else None,
                    "h_a": "h" if is_home else "a",
                    "scored": int(scored) if scored is not None else None,
                    "conceded": int(conceded) if conceded is not None else None,
                    "date": dt,
                })

            history.sort(key=lambda x: x["date"] or "")

            team_dir = OUT / "team" / t["slug"]
            team_dir.mkdir(parents=True, exist_ok=True)
            with open(team_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
                ujson.dump({ "history": history }, f, ensure_ascii=False)

            # 3) (optional) dump shots per match once
            match_dir = OUT / "match"
            match_dir.mkdir(parents=True, exist_ok=True)
            for row in history:
                if not row["id"]:
                    continue
                mid = row["id"]
                shots_path = match_dir / f"{mid}.json"
                if shots_path.exists():
                    continue
                shots = await u.get_match_shots(mid)
                with open(shots_path, "w", encoding="utf-8") as f:
                    ujson.dump(shots, f, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(dump())
