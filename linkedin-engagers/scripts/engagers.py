#!/usr/bin/env python3
"""linkedin-engagers: people who engaged with LinkedIn posts -> qualified, verified leads.

Standard library only. One run = one folder. Every stage reads and rewrites the
same engagers.csv, so any stage can be re-run or resumed.

  scrape   pull comments (+ replies) and reactions with Apify, then build
  build    rebuild engagers.csv from raw/*.json (use after fetching raw with the Apify MCP)
  exclude  drop people already in your CRM (emails / LinkedIn URLs from any CSV or TXT)
  classify-prep / classify-merge   label every engager buyer|peer|seeker|unclear (labels written by the agent)
  enrich   Prospeo bulk-enrich-person by LinkedIn URL (buyers + unclear by default)
  verify   MillionVerifier single-email API; ok and catch_all are deliverable
  report   verified.csv, buyers.csv, excluded.csv, README.md funnel
  selftest offline checks on fixtures, no network, no spend

Keys: APIFY_TOKEN, PROSPEO_API_KEY, MILLIONVERIFIER_API_KEY from the environment,
a .env file (script dir upward, cwd, ~), or ~/.config/<apify|prospeo|millionverifier>/key.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import csv
import datetime as dt
import hashlib
import json
import os
import re
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ACTORS = {
    "posts": "apimaestro~linkedin-profile-posts",
    "comments": "apimaestro~linkedin-post-comments-replies-engagements-scraper-no-cookies",
    "reactions": "apimaestro~linkedin-post-reactions",
}
APIFY_USD_PER_RESULT = 0.005  # list price on the free/bronze tier for all three actors
LABELS = ("buyer", "peer", "seeker", "unclear")
ENRICH_DEFAULT_LABELS = ("buyer", "unclear")
DELIVERABLE = ("ok", "catch_all")

FIELDS = [
    "id", "linkedin_url", "full_name", "headline", "engagement_type", "reaction_type",
    "comment_text", "comment_words", "source_post_url", "skip_reason",
    "label", "label_reason",
    "title", "company", "company_domain", "company_industry", "company_size", "country",
    "email", "prospeo_email_status", "enrich_status", "verification",
]

KEY_NAMES = {
    "apify": ("APIFY_TOKEN", "APIFY_API_TOKEN"),
    "prospeo": ("PROSPEO_API_KEY",),
    "millionverifier": ("MILLIONVERIFIER_API_KEY", "MILLION_VERIFIER_API_KEY"),
}


# ---------------------------------------------------------------- keys / io

def _env_files() -> list[Path]:
    files, d = [], Path(__file__).resolve().parent
    for _ in range(7):
        files.append(d / ".env")
        d = d.parent
    files += [Path.cwd() / ".env", Path.home() / ".env"]
    seen, out = set(), []
    for f in files:
        if f not in seen:
            seen.add(f)
            out.append(f)
    return out


def get_key(service: str) -> str | None:
    names = KEY_NAMES[service]
    for n in names:
        if os.environ.get(n):
            return os.environ[n].strip()
    for f in _env_files():
        if not f.is_file():
            continue
        for line in f.read_text(errors="ignore").splitlines():
            m = re.match(r"^\s*(?:export\s+)?([A-Z0-9_]+)\s*=\s*(.*)$", line)
            if m and m.group(1) in names and m.group(2).strip():
                return m.group(2).strip().strip("'\"")
    p = Path.home() / ".config" / service / "key"
    if p.is_file() and p.read_text().strip():
        return p.read_text().strip()
    return None


def need_key(service: str) -> str:
    k = get_key(service)
    if not k:
        die(f"missing {service} key: set {KEY_NAMES[service][0]} (env or .env) "
            f"or write it to ~/.config/{service}/key")
    return k


def die(msg: str) -> None:
    print(f"error: {msg}", file=sys.stderr)
    sys.exit(1)


def http_json(url: str, *, method: str = "GET", body=None, headers=None, timeout: int = 120,
              retries: int = 4):
    data = json.dumps(body).encode() if body is not None else None
    hdrs = {"Content-Type": "application/json", "User-Agent": "linkedin-engagers/1.0"}
    hdrs.update(headers or {})
    last = None
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read().decode("utf-8", "replace")
                return r.status, (json.loads(raw) if raw.strip() else None)
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")
            try:
                payload = json.loads(raw)
            except ValueError:
                payload = {"raw": raw[:500]}
            if e.code == 429 or e.code >= 500:
                last = (e.code, payload)
                time.sleep(min(60, 5 * (attempt + 1)))
                continue
            return e.code, payload
        except (urllib.error.URLError, TimeoutError) as e:
            last = (0, {"error": str(e)})
            time.sleep(3 * (attempt + 1))
    return last


def read_rows(run: Path) -> list[dict]:
    f = run / "engagers.csv"
    if not f.exists():
        die(f"{f} not found: run `scrape` or `build` first")
    with f.open(newline="") as fh:
        return [{k: (r.get(k) or "") for k in FIELDS} for r in csv.DictReader(fh)]


def write_csv(path: Path, rows: list[dict], fields=FIELDS) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, suffix=".tmp")
    with os.fdopen(fd, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    os.replace(tmp, path)


def load_meta(run: Path) -> dict:
    f = run / "run.json"
    return json.loads(f.read_text()) if f.exists() else {}


def save_meta(run: Path, meta: dict) -> None:
    run.mkdir(parents=True, exist_ok=True)
    (run / "run.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n")


# ---------------------------------------------------------------- normalizing

def activity_id(text: str) -> str | None:
    for pat in (r"urn:li:activity:(\d+)", r"activity[-:](\d+)", r"urn:li:ugcPost:(\d+)"):
        m = re.search(pat, text or "")
        if m:
            return m.group(1)
    return None


def poster_slug(post_url: str) -> str | None:
    """linkedin.com/posts/<slug>_title-activity-123 -> slug (the post author)."""
    m = re.search(r"linkedin\.com/posts/([^_/?#]+)_", post_url or "")
    return m.group(1).lower() if m else None


def canonical_profile(url: str) -> str:
    url = (url or "").strip()
    m = re.search(r"linkedin\.com/in/([^/?#\s]+)", url, re.I)
    if not m:
        return ""
    slug = urllib.parse.unquote(m.group(1)).strip().rstrip("/")
    return f"https://www.linkedin.com/in/{slug}"


def profile_slug(url: str) -> str:
    m = re.search(r"/in/([^/?#]+)", url or "")
    return m.group(1).lower() if m else ""


def is_urn_profile(url: str) -> bool:
    return profile_slug(url).startswith("aco")


def _norm_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").lower()).strip()


def person_id(url: str) -> str:
    return "p" + hashlib.sha1(profile_slug(url).encode()).hexdigest()[:10]


def _as_rows(body) -> list[dict]:
    if isinstance(body, list):
        return [r for r in body if isinstance(r, dict)]
    if isinstance(body, dict):
        for k in ("items", "data", "results", "comments"):
            if isinstance(body.get(k), list):
                return _as_rows(body[k])
    return []


def _s(v) -> str:
    return v.strip() if isinstance(v, str) else ""


def normalize_raw(items: list[dict], default_post: str = "") -> list[dict]:
    """Turn comment / reaction actor rows into engager rows. Replies count as comments."""
    out = []

    def comment(row, post):
        a = row.get("author") if isinstance(row.get("author"), dict) else {}
        text = _s(row.get("text"))
        out.append({
            "linkedin_url": _s(a.get("profile_url")),
            "full_name": _s(a.get("name")),
            "headline": _s(a.get("headline")),
            "engagement_type": "comment",
            "reaction_type": "",
            "comment_text": text,
            "comment_words": str(len(text.split())),
            "source_post_url": post,
        })

    for row in items:
        if "summary" in row and "author" not in row and "reactor" not in row:
            continue  # comments actor appends a summary row
        if isinstance(row.get("reactor"), dict):
            r = row["reactor"]
            meta = row.get("_metadata") if isinstance(row.get("_metadata"), dict) else {}
            out.append({
                "linkedin_url": _s(r.get("profile_url")),
                "full_name": _s(r.get("name")),
                "headline": _s(r.get("headline")),
                "engagement_type": "reaction",
                "reaction_type": _s(row.get("reaction_type")),
                "comment_text": "",
                "comment_words": "0",
                "source_post_url": _s(meta.get("post_url")) or _s(row.get("input")) or default_post,
            })
        elif isinstance(row.get("author"), dict):
            post = _s(row.get("post_input")) or default_post
            comment(row, post)
            for rep in row.get("replies") or []:
                if isinstance(rep, dict):
                    comment(rep, _s(rep.get("post_input")) or post)
    return out


def build_rows(raw_people: list[dict], exclude_slugs: set[str]) -> tuple[list[dict], dict]:
    """Dedupe on profile (a comment beats a reaction), drop post authors, flag URN profiles."""
    stats = {"raw": len(raw_people), "no_url": 0, "post_author": 0}
    by_slug: dict[str, dict] = {}
    author_names: set[str] = set()
    for p in raw_people:
        url = canonical_profile(p["linkedin_url"])
        if not url:
            stats["no_url"] += 1
            continue
        slug = profile_slug(url)
        if slug in exclude_slugs:
            stats["post_author"] += 1
            if _norm_name(p["full_name"]):
                author_names.add(_norm_name(p["full_name"]))
            continue
        p = dict(p, linkedin_url=url)
        prev = by_slug.get(slug)
        if prev is None:
            by_slug[slug] = p
        elif prev["engagement_type"] == "reaction" and p["engagement_type"] == "comment":
            by_slug[slug] = p
        elif prev["engagement_type"] == "comment" and p["engagement_type"] == "comment":
            # keep the longest comment: it says the most about the person
            if len(p["comment_text"]) > len(prev["comment_text"]):
                by_slug[slug] = p
    # Reactions usually come back as URN-only profiles (/in/ACo...) while comments carry the
    # public slug, so one person can sit under two keys. Fold each URN row into the slug row
    # with the same name (same headline first), and drop URN rows named like a post author.
    stats["merged_urn"] = 0
    named: dict[str, list[str]] = {}
    for slug, p in by_slug.items():
        if not slug.startswith("aco"):
            named.setdefault(_norm_name(p["full_name"]), []).append(slug)
    for slug in [s for s in by_slug if s.startswith("aco")]:
        p = by_slug[slug]
        nm = _norm_name(p["full_name"])
        if nm and nm in author_names:
            del by_slug[slug]
            stats["post_author"] += 1
            continue
        cands = named.get(nm, []) if nm else []
        match = [c for c in cands if by_slug[c]["headline"] == p["headline"]] or (cands if len(cands) == 1 else [])
        if match:
            keep = by_slug[match[0]]
            if keep["engagement_type"] == "reaction" and p["engagement_type"] == "comment":
                by_slug[match[0]] = dict(p, linkedin_url=keep["linkedin_url"])
            del by_slug[slug]
            stats["merged_urn"] += 1
    rows = []
    for p in by_slug.values():
        row = {k: "" for k in FIELDS}
        row.update(p)
        row["id"] = person_id(p["linkedin_url"])
        row["skip_reason"] = "urn_profile" if is_urn_profile(p["linkedin_url"]) else ""
        rows.append(row)
    rows.sort(key=lambda r: (r["engagement_type"] != "comment", r["full_name"].lower()))
    stats["unique"] = len(rows)
    stats["comments"] = sum(r["engagement_type"] == "comment" for r in rows)
    stats["reactions"] = sum(r["engagement_type"] == "reaction" for r in rows)
    stats["urn_profiles"] = sum(r["skip_reason"] == "urn_profile" for r in rows)
    return rows, stats


def merge_existing(new_rows: list[dict], old_rows: list[dict]) -> list[dict]:
    """Rebuilding keeps work already paid for (labels, emails, verification)."""
    keep = ("label", "label_reason", "title", "company", "company_domain", "company_industry",
            "company_size", "country", "email", "prospeo_email_status", "enrich_status",
            "verification")
    old = {r["id"]: r for r in old_rows}
    for r in new_rows:
        o = old.get(r["id"])
        if o:
            for k in keep:
                r[k] = o.get(k, "")
            if o.get("skip_reason") == "excluded_crm":
                r["skip_reason"] = "excluded_crm"
    return new_rows


# ---------------------------------------------------------------- apify

def apify_run(actor: str, payload: dict, token: str) -> list[dict]:
    url = f"https://api.apify.com/v2/acts/{actor}/run-sync-get-dataset-items?timeout=300&clean=1"
    status, body = http_json(url, method="POST", body=payload,
                             headers={"Authorization": f"Bearer {token}"}, timeout=330)
    if status not in (200, 201):
        die(f"apify {actor} failed [{status}]: {json.dumps(body)[:400]}")
    return _as_rows(body)


def fetch_profile_posts(username: str, n: int, token: str) -> list[dict]:
    return apify_run(ACTORS["posts"], {"username": username, "total_posts": n}, token)


def select_posts(posts: list[dict], min_age_h: float | None, max_age_h: float | None,
                 now: dt.datetime) -> list[str]:
    urls = []
    for p in posts:
        url = _s(p.get("url")).split("?")[0]
        urn = p.get("urn") if isinstance(p.get("urn"), dict) else {}
        aid = activity_id(_s(urn.get("activity_urn"))) or activity_id(url) or _s(urn.get("activity_urn"))
        # reposts carry the original post's engagers; group posts are not public
        if not aid or "groupPost" in url or _s(p.get("post_type")) == "repost":
            continue
        if min_age_h is not None or max_age_h is not None:
            posted = p.get("posted_at") if isinstance(p.get("posted_at"), dict) else {}
            ts = posted.get("timestamp")
            if not isinstance(ts, (int, float)):
                continue
            age_h = (now.timestamp() - ts / (1000 if ts > 1e12 else 1)) / 3600
            if min_age_h is not None and age_h < min_age_h:
                continue
            if max_age_h is not None and age_h >= max_age_h:
                continue
        urls.append(url or f"https://www.linkedin.com/feed/update/urn:li:activity:{aid}/")
    return urls


def fetch_post(post: str, token: str, comment_pages: int, reaction_pages: int, raw: Path) -> int:
    aid = activity_id(post) or hashlib.sha1(post.encode()).hexdigest()[:12]
    n = 0
    for page in range(1, comment_pages + 1):
        items = apify_run(ACTORS["comments"], {"postIds": [post], "page_number": page, "limit": 100,
                                               "sortOrder": "most recent"}, token)
        for it in items:
            it.setdefault("post_input", post)
        (raw / f"{aid}-comments-p{page}.json").write_text(json.dumps(items, ensure_ascii=False))
        real = [i for i in items if "author" in i]
        n += len(real)
        if len(real) < 100:
            break
    for page in range(1, reaction_pages + 1):
        items = apify_run(ACTORS["reactions"], {"post_urls": [post], "page_number": page,
                                                "limit": 100, "reaction_type": "ALL"}, token)
        for it in items:
            it.setdefault("input", post)
        (raw / f"{aid}-reactions-p{page}.json").write_text(json.dumps(items, ensure_ascii=False))
        n += len(items)
        if len(items) < 100:
            break
    return n


# ---------------------------------------------------------------- commands

def cmd_scrape(a) -> None:
    run = Path(a.run)
    raw = run / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    token = need_key("apify")
    meta = load_meta(run)
    posts = list(a.post or [])
    authors = {s for s in (poster_slug(p) for p in posts) if s}
    if a.profile:
        uname = profile_slug(a.profile) or a.profile.strip().strip("/").lower()
        authors.add(uname)
        found = fetch_profile_posts(uname, a.posts, token)
        (raw / f"profile-{uname}-posts.json").write_text(json.dumps(found, ensure_ascii=False))
        posts += select_posts(found, a.min_age_hours, a.max_age_hours, dt.datetime.now(dt.timezone.utc))
    if not posts:
        die("no posts to scrape (give --post URL or --profile; check the age window)")
    results = 0
    for p in posts:
        results += fetch_post(p, token, a.comment_pages, a.reaction_pages, raw)
        print(f"scraped {p}")
    meta.setdefault("posts", [])
    meta["posts"] = sorted(set(meta["posts"]) | set(posts))
    meta["post_authors"] = sorted(set(meta.get("post_authors", [])) | authors)
    meta["apify_results"] = meta.get("apify_results", 0) + results
    meta["created"] = meta.get("created") or dt.datetime.now().isoformat(timespec="seconds")
    save_meta(run, meta)
    print(f"apify results billed ~{results} (~${results * APIFY_USD_PER_RESULT:.2f} at list price)")
    cmd_build(a)


def cmd_build(a) -> None:
    run = Path(a.run)
    meta = load_meta(run)
    authors = set(meta.get("post_authors", [])) | {s.lower() for s in (getattr(a, "author", None) or [])}
    for p in meta.get("posts", []):
        s = poster_slug(p)
        if s:
            authors.add(s)
    raw_people = []
    files = sorted((run / "raw").glob("*.json"))
    for f in files:
        if "-posts" in f.name:
            continue
        raw_people += normalize_raw(_as_rows(json.loads(f.read_text())))
    if not raw_people:
        die(f"no engagers found in {run / 'raw'}")
    rows, stats = build_rows(raw_people, authors)
    old = (run / "engagers.csv").exists()
    if old:
        rows = merge_existing(rows, read_rows(run))
    write_csv(run / "engagers.csv", rows)
    meta["post_authors"] = sorted(authors)
    meta["build"] = stats
    save_meta(run, meta)
    enrichable = sum(1 for r in rows if not r["skip_reason"])
    print(json.dumps(stats))
    print(f"{enrichable} profiles can be enriched (~{enrichable} Prospeo credits max if you enrich all; "
          f"classify first to enrich buyers only)")


def _exclusion_sets(path: Path) -> tuple[set[str], set[str]]:
    text = path.read_text(errors="ignore")
    emails = {e.lower() for e in re.findall(r"[\w.+'-]+@[\w-]+(?:\.[\w-]+)+", text)}
    slugs = {urllib.parse.unquote(s).lower().rstrip("/") for s in re.findall(r"linkedin\.com/in/([^/?#,\s\"]+)", text)}
    return emails, slugs


def apply_exclusions(rows: list[dict], files: list[str]) -> int:
    emails, slugs = set(), set()
    for f in files:
        e, s = _exclusion_sets(Path(f))
        emails |= e
        slugs |= s
    n = 0
    for r in rows:
        if r["skip_reason"] == "excluded_crm":
            continue
        if profile_slug(r["linkedin_url"]) in slugs or (r["email"] and r["email"].lower() in emails):
            r["skip_reason"] = "excluded_crm"
            n += 1
    return n


def cmd_exclude(a) -> None:
    run = Path(a.run)
    rows = read_rows(run)
    meta = load_meta(run)
    files = sorted(set(meta.get("exclude_files", [])) | {str(Path(f).resolve()) for f in a.file})
    n = apply_exclusions(rows, files)
    meta["exclude_files"] = files
    save_meta(run, meta)
    write_csv(run / "engagers.csv", rows)
    print(f"excluded {n} people already in your CRM ({sum(r['skip_reason'] == 'excluded_crm' for r in rows)} total)")


RUBRIC = """# Classify LinkedIn engagers

ICP (who the user sells to):
{icp}

Label every person in each batch file. Judge from headline, comment, engagement type and, when
present, company fields. One label each:

- buyer: matches the ICP above. A decision-maker or budget owner at a company that fits.
- peer: sells something similar or to the same buyers: agencies, consultants, freelancers,
  coaches, creators, tool vendors, recruiters. Includes competitors of the user.
- seeker: wants the freebie or a job, not a purchase: students, job seekers, "interested"/
  keyword-only comments with no business context, generic engagement pods.
- unclear: not enough signal to tell. Prefer this to a guess.

Output for batch-NNN.json is labels-NNN.json in the same folder: a JSON list of
{{"id": "...", "label": "buyer|peer|seeker|unclear", "reason": "<= 12 words, cite the headline or comment"}}
Every id in the batch appears exactly once.
"""


def cmd_classify_prep(a) -> None:
    run = Path(a.run)
    rows = read_rows(run)
    icp = Path(a.icp_file).read_text() if a.icp_file else (a.icp or "")
    if not icp.strip():
        die("give the ICP with --icp 'text' or --icp-file path")
    out = run / "classify"
    out.mkdir(exist_ok=True)
    todo = [r for r in rows if r["skip_reason"] != "excluded_crm" and (a.relabel or not r["label"])]
    (out / "RUBRIC.md").write_text(RUBRIC.format(icp=icp.strip()))
    for old in out.glob("batch-*.json"):
        old.unlink()
    n = 0
    for i in range(0, len(todo), a.batch):
        chunk = [{k: r[k] for k in ("id", "full_name", "headline", "engagement_type", "comment_text",
                                     "title", "company", "company_industry", "company_size") if r[k]}
                 for r in todo[i:i + a.batch]]
        n += 1
        (out / f"batch-{n:03d}.json").write_text(json.dumps(chunk, indent=1, ensure_ascii=False))
    print(f"{len(todo)} people in {n} batch file(s) under {out}; rubric: {out / 'RUBRIC.md'}")


def cmd_classify_merge(a) -> None:
    run = Path(a.run)
    rows = read_rows(run)
    by_id = {r["id"]: r for r in rows}
    merged, bad, missing = 0, [], []
    for f in sorted((run / "classify").glob("labels-*.json")):
        for item in json.loads(f.read_text()):
            rid, label = item.get("id"), str(item.get("label", "")).lower().strip()
            if rid not in by_id or label not in LABELS:
                bad.append(f"{f.name}:{rid}:{label}")
                continue
            by_id[rid]["label"] = label
            by_id[rid]["label_reason"] = str(item.get("reason", ""))[:200]
            merged += 1
    for f in sorted((run / "classify").glob("batch-*.json")):
        for item in json.loads(f.read_text()):
            if not by_id.get(item["id"], {}).get("label"):
                missing.append(item["id"])
    write_csv(run / "engagers.csv", rows)
    print(f"merged {merged} labels; invalid {len(bad)}; still unlabeled {len(missing)}")
    if bad:
        print("invalid: " + ", ".join(bad[:20]))
    if missing:
        print("unlabeled ids: " + ", ".join(missing[:20]))
        sys.exit(2)


def _enrich_targets(rows: list[dict], labels: tuple[str, ...] | None) -> list[dict]:
    return [r for r in rows
            if not r["skip_reason"] and not r["enrich_status"]
            and (labels is None or r["label"] in labels)]


def cmd_enrich(a) -> None:
    run = Path(a.run)
    rows = read_rows(run)
    labels = None if a.all else tuple(a.labels.split(","))
    if labels and not any(r["label"] for r in rows):
        die("nobody is labeled yet: run classify-prep/classify-merge first, or pass --all")
    targets = _enrich_targets(rows, labels)
    if a.limit:
        targets = targets[:a.limit]
    print(f"{len(targets)} profiles to enrich (max {len(targets)} Prospeo credits; no charge when not found)")
    if not targets:
        return
    if len(targets) > a.confirm_over and not a.yes:
        die(f"more than {a.confirm_over} profiles: re-run with --yes to spend the credits")
    key = need_key("prospeo")
    found = 0
    for i in range(0, len(targets), 50):
        chunk = targets[i:i + 50]
        body = {"only_verified_email": False,
                "data": [{"identifier": r["id"], "linkedin_url": r["linkedin_url"]} for r in chunk]}
        status, resp = http_json("https://api.prospeo.io/bulk-enrich-person", method="POST", body=body,
                                 headers={"X-KEY": key}, timeout=120)
        if status != 200 or not isinstance(resp, dict) or resp.get("error"):
            code = (resp or {}).get("error_code") if isinstance(resp, dict) else resp
            write_csv(run / "engagers.csv", rows)
            die(f"prospeo failed [{status}] {code}; progress saved, re-run enrich to continue")
        by_id = {r["id"]: r for r in chunk}
        for m in resp.get("matched") or []:
            r = by_id.get(m.get("identifier"))
            if not r:
                continue
            person = m.get("person") or {}
            company = m.get("company") or {}
            em = person.get("email") or {}
            email = _s(em.get("email")) if em.get("revealed", True) else ""
            r["email"] = "" if "*" in email else email
            r["prospeo_email_status"] = _s(em.get("status"))
            r["title"] = _s(person.get("current_job_title")) or r["title"]
            r["company"] = _s(company.get("name"))
            r["company_domain"] = _s(company.get("domain")) or _s(company.get("website"))
            r["company_industry"] = _s(company.get("industry"))
            r["company_size"] = _s(company.get("employee_range")) or str(company.get("employee_count") or "")
            loc = person.get("location") or {}
            r["country"] = _s(loc.get("country"))
            r["enrich_status"] = "email_found" if r["email"] else "matched_no_email"
            found += bool(r["email"])
        for ident in (resp.get("not_matched") or []) + (resp.get("invalid_datapoints") or []):
            if ident in by_id and not by_id[ident]["enrich_status"]:
                by_id[ident]["enrich_status"] = "not_found"
        for r in chunk:
            r["enrich_status"] = r["enrich_status"] or "not_found"
        write_csv(run / "engagers.csv", rows)
        print(f"  batch {i // 50 + 1}: {sum(bool(r['email']) for r in chunk)}/{len(chunk)} emails")
    meta = load_meta(run)
    if meta.get("exclude_files"):
        n = apply_exclusions(rows, meta["exclude_files"])
        if n:
            print(f"excluded {n} more by email after enrichment")
        write_csv(run / "engagers.csv", rows)
    print(f"emails found: {found}/{len(targets)}")


def _mv_check(email: str, key: str) -> str:
    q = urllib.parse.urlencode({"api": key, "email": email, "timeout": 20})
    status, body = http_json(f"https://api.millionverifier.com/api/v3/?{q}", timeout=60)
    if status != 200 or not isinstance(body, dict):
        return "error"
    if body.get("error"):
        raise RuntimeError(str(body["error"]))
    return str(body.get("result") or "unknown").lower()


def cmd_verify(a) -> None:
    run = Path(a.run)
    rows = read_rows(run)
    todo = [r for r in rows if r["email"] and r["skip_reason"] != "excluded_crm"
            and (not r["verification"] or r["verification"] == "error")]
    print(f"{len(todo)} emails to verify (~{len(todo)} MillionVerifier credits)")
    if not todo:
        return
    key = need_key("millionverifier")
    done = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as pool:
        futs = {pool.submit(_mv_check, r["email"], key): r for r in todo}
        try:
            for fut in concurrent.futures.as_completed(futs):
                futs[fut]["verification"] = fut.result()
                done += 1
        except RuntimeError as e:
            write_csv(run / "engagers.csv", rows)
            die(f"millionverifier: {e}; progress saved ({done} done)")
    write_csv(run / "engagers.csv", rows)
    counts: dict[str, int] = {}
    for r in todo:
        counts[r["verification"]] = counts.get(r["verification"], 0) + 1
    print(json.dumps(counts))


def funnel(rows: list[dict]) -> dict:
    active = [r for r in rows if r["skip_reason"] != "excluded_crm"]
    lab = {l: sum(r["label"] == l for r in active) for l in LABELS}
    deliverable = [r for r in active if r["verification"] in DELIVERABLE]
    return {
        "engagers": len(rows),
        "comments": sum(r["engagement_type"] == "comment" for r in rows),
        "reactions": sum(r["engagement_type"] == "reaction" for r in rows),
        "excluded_crm": sum(r["skip_reason"] == "excluded_crm" for r in rows),
        "urn_profiles": sum(r["skip_reason"] == "urn_profile" for r in rows),
        "labeled": sum(bool(r["label"]) for r in active),
        **{f"label_{k}": v for k, v in lab.items()},
        "enriched": sum(bool(r["enrich_status"]) for r in active),
        "emails_found": sum(bool(r["email"]) for r in active),
        "verified_deliverable": len(deliverable),
        "deliverable_buyers": sum(r["label"] == "buyer" for r in deliverable),
    }


def cmd_report(a) -> None:
    run = Path(a.run)
    rows = read_rows(run)
    meta = load_meta(run)
    active = [r for r in rows if r["skip_reason"] != "excluded_crm"]
    verified = [r for r in active if r["verification"] in DELIVERABLE]
    buyers = [r for r in verified if r["label"] == "buyer"]
    write_csv(run / "verified.csv", verified)
    write_csv(run / "buyers.csv", buyers)
    write_csv(run / "excluded.csv", [r for r in rows if r["skip_reason"] == "excluded_crm"])
    f = funnel(rows)
    meta["funnel"] = f
    save_meta(run, meta)
    lines = [f"# LinkedIn engagers run: {run.name}", "",
             "Posts:", *[f"- {p}" for p in meta.get("posts", [])], "",
             "| Stage | Count |", "|---|---|", *[f"| {k} | {v} |" for k, v in f.items()], "",
             "Files: engagers.csv (everyone), verified.csv (ok + catch_all, not excluded), "
             "buyers.csv (verified buyers), excluded.csv (already in CRM).", ""]
    (run / "README.md").write_text("\n".join(lines))
    print(json.dumps(f, indent=1))


# ---------------------------------------------------------------- selftest

def cmd_selftest(_a) -> None:
    post = "https://www.linkedin.com/posts/jane-doe_ai-activity-7300000000000000001-AbCd"
    comments = [
        {"author": {"name": "Ada Buyer", "headline": "CEO at Acme Logistics",
                    "profile_url": "https://www.linkedin.com/in/ada-buyer/?trk=x"},
         "text": "We need this for our SDR team", "post_input": post,
         "replies": [{"author": {"name": "Jane Doe", "headline": "Creator",
                                 "profile_url": "https://www.linkedin.com/in/jane-doe"},
                      "text": "Sent!"}]},
        {"author": {"name": "Bob Peer", "headline": "Founder, outbound agency",
                    "profile_url": "https://www.linkedin.com/in/bob-peer"}, "text": "TEAM",
         "post_input": post},
        {"summary": {"totalComments": 3}},
    ]
    reactions = [
        {"reactor": {"name": "Ada Buyer", "headline": "CEO at Acme Logistics",
                     "profile_url": "https://www.linkedin.com/in/ada-buyer"},
         "reaction_type": "LIKE", "_metadata": {"post_url": post}},
        {"reactor": {"name": "Urn Person", "headline": "x",
                     "profile_url": "https://www.linkedin.com/in/ACoAAB123"},
         "reaction_type": "LIKE", "input": post},
        {"reactor": {"name": "No Url", "headline": "x", "profile_url": ""}, "reaction_type": "LIKE"},
        {"reactor": {"name": "Bob Peer", "headline": "Founder, outbound agency",
                     "profile_url": "https://www.linkedin.com/in/ACoAABOB"}, "reaction_type": "LIKE"},
        {"reactor": {"name": "Jane Doe", "headline": "Creator",
                     "profile_url": "https://www.linkedin.com/in/ACoAAJANE"}, "reaction_type": "LIKE"},
    ]
    people = normalize_raw(comments) + normalize_raw(reactions)
    assert len(people) == 8, len(people)
    rows, stats = build_rows(people, {poster_slug(post)})
    ids = {r["full_name"]: r for r in rows}
    assert poster_slug(post) == "jane-doe"
    assert stats["post_author"] == 2 and stats["no_url"] == 1, stats  # author's reply + URN reaction
    assert stats["merged_urn"] == 1, stats  # Bob's URN reaction folded into his comment
    assert set(ids) == {"Ada Buyer", "Bob Peer", "Urn Person"}, set(ids)
    assert ids["Ada Buyer"]["engagement_type"] == "comment", "comment must beat reaction"
    assert ids["Ada Buyer"]["linkedin_url"] == "https://www.linkedin.com/in/ada-buyer"
    assert ids["Urn Person"]["skip_reason"] == "urn_profile"
    assert ids["Bob Peer"]["comment_words"] == "1"
    assert activity_id(post) == "7300000000000000001"
    assert activity_id("https://www.linkedin.com/feed/update/urn:li:activity:42/") == "42"
    with tempfile.TemporaryDirectory() as td:
        run = Path(td)
        crm = run / "crm.csv"
        crm.write_text("email,linkedin\nx@y.com,https://www.linkedin.com/in/bob-peer/\n")
        n = apply_exclusions(rows, [str(crm)])
        assert n == 1 and ids["Bob Peer"]["skip_reason"] == "excluded_crm"
        ids["Ada Buyer"]["label"] = "buyer"
        ids["Urn Person"]["label"] = "unclear"
        targets = _enrich_targets(rows, ENRICH_DEFAULT_LABELS)
        assert [t["full_name"] for t in targets] == ["Ada Buyer"], targets
        ids["Ada Buyer"].update(email="ada@acme.com", enrich_status="email_found", verification="ok")
        write_csv(run / "engagers.csv", rows)
        back = read_rows(run)
        assert len(back) == 3 and back[0]["email"] == "ada@acme.com"
        rebuilt = merge_existing(build_rows(people, {"jane-doe"})[0], back)
        r2 = {r["full_name"]: r for r in rebuilt}
        assert r2["Ada Buyer"]["verification"] == "ok" and r2["Bob Peer"]["skip_reason"] == "excluded_crm"
        f = funnel(rows)
        assert f["deliverable_buyers"] == 1 and f["excluded_crm"] == 1, f
    print(json.dumps({"ok": True, "checks": 16}))


# ---------------------------------------------------------------- cli

def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("scrape", help="pull engagers with Apify, then build")
    s.add_argument("run", help="run folder, e.g. output/linkedin-engagers/2026-11-09-jane-doe")
    s.add_argument("--post", action="append", help="post URL or activity id (repeatable)")
    s.add_argument("--profile", help="profile URL or username: scrape its recent posts")
    s.add_argument("--posts", type=int, default=5, help="recent posts to pull for --profile (default 5)")
    s.add_argument("--min-age-hours", type=float, help="profile mode: skip posts younger than this")
    s.add_argument("--max-age-hours", type=float, help="profile mode: skip posts older than this")
    s.add_argument("--comment-pages", type=int, default=3, help="100 comments per page (default 3)")
    s.add_argument("--reaction-pages", type=int, default=3, help="100 reactions per page (default 3)")
    s.set_defaults(fn=cmd_scrape)

    b = sub.add_parser("build", help="rebuild engagers.csv from raw/*.json")
    b.add_argument("run")
    b.add_argument("--author", action="append", help="profile slug of a post author to drop (repeatable)")
    b.set_defaults(fn=cmd_build)

    e = sub.add_parser("exclude", help="drop people already in your CRM")
    e.add_argument("run")
    e.add_argument("--file", action="append", required=True, help="CSV/TXT with emails and/or LinkedIn URLs")
    e.set_defaults(fn=cmd_exclude)

    cp = sub.add_parser("classify-prep", help="write label batches + rubric for the agent")
    cp.add_argument("run")
    cp.add_argument("--icp", help="who the user sells to, in one or two sentences")
    cp.add_argument("--icp-file")
    cp.add_argument("--batch", type=int, default=60)
    cp.add_argument("--relabel", action="store_true")
    cp.set_defaults(fn=cmd_classify_prep)

    cm = sub.add_parser("classify-merge", help="merge classify/labels-*.json into engagers.csv")
    cm.add_argument("run")
    cm.set_defaults(fn=cmd_classify_merge)

    en = sub.add_parser("enrich", help="Prospeo emails by LinkedIn URL")
    en.add_argument("run")
    en.add_argument("--labels", default=",".join(ENRICH_DEFAULT_LABELS))
    en.add_argument("--all", action="store_true", help="ignore labels, enrich everyone enrichable")
    en.add_argument("--limit", type=int)
    en.add_argument("--confirm-over", type=int, default=100)
    en.add_argument("--yes", action="store_true")
    en.set_defaults(fn=cmd_enrich)

    v = sub.add_parser("verify", help="MillionVerifier every found email")
    v.add_argument("run")
    v.add_argument("--workers", type=int, default=5)
    v.set_defaults(fn=cmd_verify)

    r = sub.add_parser("report", help="write the output CSVs and README funnel")
    r.add_argument("run")
    r.set_defaults(fn=cmd_report)

    t = sub.add_parser("selftest", help="offline checks, no network")
    t.set_defaults(fn=cmd_selftest)

    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
