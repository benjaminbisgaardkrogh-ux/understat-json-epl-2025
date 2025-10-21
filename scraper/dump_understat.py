import asyncio, json, ujson, re
from pathlib import Path
import aiohttp
from understat import Understat
from collections import defaultdict

# ========= SETTINGS =========
LEAGUE = "EPL"      # Understat league code
SEASON = 2025       # 2025/26 season
DUMP_SHOTS = True   # write /match/<id>.json
# ============================

OUT = Path(__file__).resolve().parents[1] / "public"
OUT.mkdir(parents=True, exist_ok=True)

# Two-way slug aliases so BOTH URLs exist and contain the same data
SLUG_ALIAS_PAIRS = [
    ("manchester-city", "man-city"),
    ("manchester-united", "man-utd"),
    ("manchester-united", "man-united"),
    ("tottenham", "spurs"),
    ("bournemouth", "bmouth"),
    ("sheffield-united", "sheff-utd"),
    ("sheffield-united", "sheffield-utd"),
    ("crystal-palace", "palace"),
    ("nottingham-forest", "forest"),
    ("wolverhampton", "wolves"),
    ("newcastle-united", "newcastle"),
    ("west-ham", "west-ham-united"),  # <-- West Ham pair
]

def slugify(s: str) -> str:
    s = s.lower().replace("&", "and")
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s

def alias_slugs_for_slug(official_slug: str):
    """Return alias slugs that should mirror this official slug."""
    out = set()
    s = official_slug
    for a, b in SLUG_ALIAS_PAIRS:
        if s == a:
            out.add(b)
        if s == b:
            out.add(a)
    return sorted(out)

async def dump():
    async with aiohttp.ClientSession() as session:
        u = Understat(session)

        # Always fetch Premier League only (EPL)
        results = await u.get_league_results("epl", SEASON)


        # 2) Build per-team histories directly from results (key: official slug)
        histories = defaultdict(list)   # slug -> list of rows
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

            mid = int(mid)
            h_goals = int(h_goals)
            a_goals = int(a_goals)

            titles_set.add(h_title)
            titles_set.add(a_title)

            h_slug = slugify(h_title)
            a_slug = slugify(a_title)

            histories[h_slug].append({
                "id": mid, "h_a": "h",
                "scored": h_goals, "conceded": a_goals,
                "date": dt
            })
            histories[a_slug].append({
                "id": mid, "h_a": "a",
                "scored": a_goals, "conceded": h_goals,
                "date": dt
            })

        # 3) Write team files for OFFICIAL slug + ALIASES
        for title in sorted(titles_set):
            official_slug = slugify(title)
            team_hist = histories.get(official_slug, [])
            team_hist.sort(key=lambda x: x["date"] or "")

            # official
            team_dir = OUT / "team" / official_slug
            team_dir.mkdir(parents=True, exist_ok=True)
            with open(team_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
                ujson.dump({"history": team_hist}, f, ensure_ascii=False)

            # aliases (write identical JSON so both URLs work)
            for a_slug in alias_slugs_for_slug(official_slug):
                a_dir = OUT / "team" / a_slug
                a_dir.mkdir(parents=True, exist_ok=True)
                with open(a_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
                    ujson.dump({"history": team_hist}, f, ensure_ascii=False)

        # 4) League teams list
        teams_out = [{"id": None, "title": t, "slug": slugify(t)} for t in sorted(titles_set)]
        league_dir = OUT / "league" / LEAGUE
        league_dir.mkdir(parents=True, exist_ok=True)
        with open(league_dir / f"{SEASON}.json", "w", encoding="utf-8") as f:
            json.dump({"teams": teams_out}, f, ensure_ascii=False)

        # 5) Dump per-match shots once
        if DUMP_SHOTS:
            match_dir = OUT / "match"
            match_dir.mkdir(parents=True, exist_ok=True)
            seen = set()
            for r in results:
                mid = r.get("id")
                if not mid: continue
                mid = int(mid)
                if mid in seen: continue
                seen.add(mid)
                shots_path = match_dir / f"{mid}.json"
                if shots_path.exists(): continue
                shots = await u.get_match_shots(mid)  # {h:[], a:[]}
                with open(shots_path, "w", encoding="utf-8") as f:
                    ujson.dump(shots, f, ensure_ascii=False)

if __name__ == "__main__":
    asyncio.run(dump())
