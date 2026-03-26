# MedStar Instagram Monitor

A web-based tool for monitoring Instagram influencer posts and identifying top-performing content based on engagement metrics.

## Features

- **Track 20+ Instagram profiles** — Add any public Instagram account by username
- **Automated scraping** — Pull latest posts with likes, comments, views, and engagement rates
- **Interactive dashboard** — Dark-themed web UI with charts, filters, and sortable tables
- **Top Posts ranking** — See the highest-performing posts across all tracked profiles
- **Hooks & Caption Analysis** — Extract the opening lines from top posts to inspire your own content
- **Category tagging** — Organize profiles by niche (Health, Fitness, Business, etc.)
- **Export data** — JSON export of all collected data

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Run the app
```bash
python app.py
```

### 3. Open the dashboard
Go to **http://localhost:5000** in your browser.

### 4. Add profiles
Click **+ Add Profile** and enter Instagram usernames (e.g., `garyvee`, `hubermanlab`).

### 5. Scrape posts
Click **Scrape** on individual profiles, or **Scrape All Profiles** to pull the latest posts for everyone.

## Dashboard Pages

| Page | Description |
|------|-------------|
| **Overview** | Stats summary, engagement charts, and top 5 posts |
| **Top Posts** | Ranked list of best-performing posts with filtering |
| **Hooks & Captions** | Opening lines from top posts — great for content inspiration |
| **Profiles** | Manage tracked accounts, scrape individually, or remove |
| **All Posts** | Full searchable/filterable table of every collected post |

## How It Works

1. **Instaloader** scrapes public Instagram profiles (no login required for public accounts)
2. Data is stored in a local **SQLite** database (`monitor.db`)
3. **Flask** serves a web dashboard at localhost:5000
4. Engagement rate is calculated as: `(likes + comments) / followers × 100`

## Important Notes

- Only works with **public** Instagram profiles
- Instagram may rate-limit scraping if done too aggressively — the tool includes polite delays between requests
- For best results, scrape during off-peak hours and avoid scraping more than ~50 profiles in one session
- No Instagram login is needed for public profile scraping
