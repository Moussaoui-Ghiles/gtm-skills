# linkedin-engagers

A Claude Code skill that turns the people who engage with LinkedIn posts into qualified, verified leads.

Give it a post (yours, a competitor's, an influencer's) or a profile. It pulls every commenter, reply and reaction, labels each person against your ICP (buyer, peer, seeker, unclear), finds emails for the buyers only, verifies them, and writes clean CSVs.

## Why qualify before you enrich

Most engagers on a lead-gen post are not buyers. They are agencies, creators, tool vendors and people who want the freebie. Paying for an email on each of them burns credits. This skill labels everyone first from their headline and comment, then sends only `buyer` and `unclear` profiles to the email finder.

## Pipeline

```
scrape     Apify: comments (+ replies) and reactions, up to 300 each per post
build      dedupe on profile (a comment beats a reaction), drop the post author, flag URN-only profiles
exclude    drop anyone already in your CRM (by LinkedIn URL now, by email after enrichment)
classify   Claude labels every engager against your ICP (no extra API key: the agent does it)
enrich     Prospeo bulk-enrich-person by LinkedIn URL, buyers + unclear only
verify     MillionVerifier: ok and catch_all are deliverable
report     engagers.csv, verified.csv, buyers.csv, excluded.csv, README.md funnel
```

Standard-library Python 3. No packages, no database. One run = one folder you can reopen.

## Install

```sh
git clone <this repo>
cp -r linkedin-engagers ~/.claude/skills/        # or <project>/.claude/skills/
python3 ~/.claude/skills/linkedin-engagers/scripts/engagers.py selftest
```

Keys, from the environment, a `.env` file, or `~/.config/<service>/key`:

| Key | Used by | Needed for |
|---|---|---|
| `APIFY_TOKEN` | scrape | pulling engagers (or use the Apify MCP and `build`) |
| `PROSPEO_API_KEY` | enrich | emails by LinkedIn URL |
| `MILLIONVERIFIER_API_KEY` | verify | email verification |

## Use

In Claude Code: "get the engagers of <post url>, my ICP is <who you sell to>". The skill walks the steps and stops for your go before spending more than 100 Prospeo credits.

By hand:

```sh
S=~/.claude/skills/linkedin-engagers/scripts/engagers.py
R=linkedin-engagers/2026-11-09-jane-doe
python3 $S scrape $R --post https://www.linkedin.com/posts/jane-doe_...
python3 $S exclude $R --file hubspot-export.csv
python3 $S classify-prep $R --icp "Founders of B2B SaaS, 10-200 staff, selling \$10K+ deals"
#   label classify/batch-*.json into classify/labels-*.json (Claude does this step)
python3 $S classify-merge $R
python3 $S enrich $R
python3 $S verify $R
python3 $S report $R
```

Watch creators daily: `scrape <run> --profile <username> --posts 20 --min-age-hours 24 --max-age-hours 72`, scheduled with cron.

## Costs (list prices)

- Apify: about $0.005 per comment or reaction scraped.
- Prospeo: 1 credit per email found. Not found is free, and re-enriching within 90 days is free.
- MillionVerifier: 1 credit per email checked.

## Limits

- Reactions come back as URN-only profiles (`/in/ACo…`): name and headline, no public URL. They get labeled, but only commenters can be matched to an email.
- LinkedIn's terms restrict scraping, and emailing people you scraped falls under GDPR and similar laws where they live. Check both for your use.

## Credit

Rewritten from [graphed-inc/marketing-skills `enrich-linkedin-engagers`](https://github.com/graphed-inc/marketing-skills/tree/main/skills/enrich-linkedin-engagers) (MIT). Changes: no Graphed platform, Postgres or CLI; Prospeo instead of Apollo; reply authors included; post author dropped; classify-before-enrich step; resumable CSV state.
