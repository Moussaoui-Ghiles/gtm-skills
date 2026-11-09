---
name: linkedin-engagers
description: Turn the people who commented on or reacted to LinkedIn posts (a competitor's, an influencer's, or your own) into qualified, verified leads. Use when the user gives a LinkedIn post or profile and wants its engagers scraped, qualified, enriched with emails, or verified, or wants to watch a creator's new posts on a schedule.
argument-hint: "<linkedin post url | profile url> [icp]"
---

# LinkedIn engagers → qualified leads

One post or profile in. A run folder out: every engager, labeled against the user's ICP, with verified emails for the buyers only.

The order is the point: **qualify before you enrich**. Engagers are mostly peers (agencies, creators, vendors) and freebie seekers, so labeling first from headline and comment means Prospeo credits go to buyers.

Everything runs through `scripts/engagers.py` (Python 3, standard library only). Each stage reads and rewrites `<run>/engagers.csv`, so any stage can be re-run, and paid work already done is kept. Run `python3 scripts/engagers.py <command> -h` for flags.

## Before you start

1. Get the input: a post URL, or a profile (URL or username) plus how many recent posts (default 5). Get the ICP in one or two sentences: who the user sells to and who counts as a competitor. Ask for whichever is missing.
2. Pick the run folder: `<base>/<yyyy-mm-dd>-<post-author-or-profile-slug>`. `<base>` is the user's staging folder if their project has one (BizOps: `output/linkedin-engagers/`), else `linkedin-engagers/`.
3. Keys the script resolves itself (env, `.env`, or `~/.config/<service>/key`): `APIFY_TOKEN`, `PROSPEO_API_KEY`, `MILLIONVERIFIER_API_KEY`. Missing keys stop only the stage that needs them.

## Steps

1. **Scrape.** `scrape <run> --post <url>` (repeatable) or `scrape <run> --profile <username> --posts 5`. It pulls up to 300 comments with their replies and 300 reactions per post, then builds `engagers.csv`.
   - No `APIFY_TOKEN` but the Apify MCP is connected: call the same actors there (`apimaestro/linkedin-post-comments-replies-engagements-scraper-no-cookies` with `postIds`, `limit: 100`; `apimaestro/linkedin-post-reactions` with `post_urls`, `limit: 100`; page with `page_number` while a page returns 100), save each dataset's items as `<run>/raw/<activity-id>-comments-p<N>.json` / `-reactions-p<N>.json`, write `{"posts": [<post urls>]}` to `<run>/run.json`, then run `build <run>`.
   - Done when the build line prints `unique`, `comments`, `reactions`, `urn_profiles`. Tell the user those counts.
2. **Exclude** (when the user has a CRM export): `exclude <run> --file contacts.csv`. Any CSV or TXT works; emails and LinkedIn URLs are found anywhere in it. It runs again by email after enrichment. Done when it prints the excluded count.
3. **Classify.** `classify-prep <run> --icp "<icp>"` writes `classify/RUBRIC.md` and `classify/batch-NNN.json`. Label every batch into `classify/labels-NNN.json` following the rubric (one subagent per few batches keeps the main context clean), then `classify-merge <run>`. Done when it prints `invalid 0` and `still unlabeled 0` (it exits 2 otherwise).
4. **Enrich.** `enrich <run>` sends `buyer` and `unclear` people to Prospeo by LinkedIn URL (`--labels` to change, `--all` to ignore labels). Over 100 profiles it stops until you re-run with `--yes`; show the user the count first and get their go. Done when it prints `emails found: X/Y`.
5. **Verify.** `verify <run>` checks every found email with MillionVerifier. `ok` and `catch_all` are deliverable; everything else stays in the file with its result. Done when it prints the result counts.
6. **Report.** `report <run>` writes `verified.csv`, `buyers.csv`, `excluded.csv` and a `README.md` funnel. Reply with the funnel table, the run folder path, and the top buyers by name and headline.

## Known limits

- **Reactions come back as URN profiles** (`/in/ACo…`) with name and headline but no public slug, so Prospeo cannot match them by URL. They are still labeled; only commenters get emails. Say this when reporting counts.
- The post author's own replies are dropped automatically, as are people with no profile URL.
- Same Prospeo record within 90 days is free; a not-found costs nothing.

## Watching creators on a schedule

`scrape <run> --profile <username> --posts 20 --min-age-hours 24 --max-age-hours 72` only takes posts that are 1 to 3 days old (engagement has mostly landed, and each post is picked up once a day). Schedule it with cron or any job runner, one run folder per day, followed by the same classify, enrich, verify and report steps.
