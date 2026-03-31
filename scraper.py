"""
Instagram Scraper Module
Scrapes public Instagram profiles for post data and engagement metrics.
Uses requests with Instagram's web API for reliable cloud-based extraction.
"""

import requests as req
import time
import random
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

# ── Shared browser-like headers ──────────────────────────────────────
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "X-IG-App-ID": "936619743392459",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.instagram.com/",
    "Origin": "https://www.instagram.com",
}

# ── Database helpers ─────────────────────────────────────────────────

def _get_db_path():
    """Determine the best location for the database file."""
    env_path = os.environ.get("IG_MONITOR_DB")
    if env_path:
        return env_path
    default = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    try:
        import tempfile
        test_path = default + ".test"
        c = sqlite3.connect(test_path)
        c.execute("CREATE TABLE _t (id INTEGER)")
        c.close()
        os.remove(test_path)
        return default
    except Exception:
        fallback_dir = os.path.join(os.path.expanduser("~"), ".ig-monitor")
        os.makedirs(fallback_dir, exist_ok=True)
        return os.path.join(fallback_dir, "monitor.db")

DB_PATH = _get_db_path()


def get_db():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    """Initialize the SQLite database with required tables."""
    conn = get_db()
    cursor = conn.cursor()
    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            full_name TEXT,
            follower_count INTEGER DEFAULT 0,
            following_count INTEGER DEFAULT 0,
            post_count INTEGER DEFAULT 0,
            bio TEXT,
            profile_pic_url TEXT,
            is_verified INTEGER DEFAULT 0,
            category TEXT DEFAULT 'Uncategorized',
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_scraped TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            shortcode TEXT UNIQUE NOT NULL,
            post_url TEXT,
            caption TEXT,
            post_type TEXT,
            likes INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0,
            saves INTEGER DEFAULT 0,
            video_views INTEGER DEFAULT 0,
            engagement_rate REAL DEFAULT 0.0,
            posted_at TIMESTAMP,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            thumbnail_url TEXT,
            is_video INTEGER DEFAULT 0,
            hashtags TEXT,
            FOREIGN KEY (profile_id) REFERENCES profiles(id)
        );
        CREATE TABLE IF NOT EXISTS scrape_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER,
            status TEXT,
            message TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles(id)
        );
        CREATE TABLE IF NOT EXISTS follower_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            profile_id INTEGER NOT NULL,
            follower_count INTEGER DEFAULT 0,
            following_count INTEGER DEFAULT 0,
            post_count INTEGER DEFAULT 0,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (profile_id) REFERENCES profiles(id)
        );
        CREATE INDEX IF NOT EXISTS idx_posts_profile ON posts(profile_id);
        CREATE INDEX IF NOT EXISTS idx_posts_engagement ON posts(engagement_rate DESC);
        CREATE INDEX IF NOT EXISTS idx_posts_likes ON posts(likes DESC);
        CREATE INDEX IF NOT EXISTS idx_posts_posted ON posts(posted_at DESC);
        CREATE TABLE IF NOT EXISTS hashtag_library (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hashtag TEXT UNIQUE NOT NULL,
            category TEXT NOT NULL,
            popularity TEXT DEFAULT 'medium',
            post_volume TEXT,
            description TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS content_ideas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            talking_points TEXT DEFAULT '',
            status TEXT DEFAULT 'pending',
            sort_order INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS hook_ideas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hook_text TEXT NOT NULL,
            category TEXT DEFAULT 'General',
            source_post_url TEXT,
            source_username TEXT,
            source_engagement REAL DEFAULT 0,
            hook_score INTEGER DEFAULT 0,
            content_idea_id INTEGER,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (content_idea_id) REFERENCES content_ideas(id)
        );
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            display_name TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            is_active INTEGER DEFAULT 1,
            twofa_code TEXT,
            twofa_expires TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_login TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_snapshots_profile ON follower_snapshots(profile_id);
        CREATE INDEX IF NOT EXISTS idx_snapshots_date ON follower_snapshots(recorded_at);
        CREATE INDEX IF NOT EXISTS idx_hashtag_lib_cat ON hashtag_library(category);
        CREATE INDEX IF NOT EXISTS idx_hook_ideas_status ON hook_ideas(status);
        CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
    """)
    conn.commit()

    # ── Migrate existing posts table if needed ──
    post_cols = [r["name"] for r in conn.execute("PRAGMA table_info(posts)").fetchall()]
    if "saves" not in post_cols:
        conn.execute("ALTER TABLE posts ADD COLUMN saves INTEGER DEFAULT 0")
        conn.commit()

    # ── Migrate existing hook_ideas table if needed ──
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(hook_ideas)").fetchall()]
    if "hook_score" not in cols:
        conn.execute("ALTER TABLE hook_ideas ADD COLUMN hook_score INTEGER DEFAULT 0")
    if "content_idea_id" not in cols:
        conn.execute("ALTER TABLE hook_ideas ADD COLUMN content_idea_id INTEGER")
    conn.commit()

    # Seed the hashtag library if empty
    _seed_hashtag_library(conn)
    # Import content ideas from CSV if empty
    _seed_content_ideas(conn)
    conn.close()


def _seed_content_ideas(conn):
    """Import content ideas and hooks from the CSV file if the table is empty."""
    import csv as _csv
    existing = conn.execute("SELECT COUNT(*) as c FROM content_ideas").fetchone()["c"]
    if existing > 0:
        return

    csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "content_ideas.csv")
    if not os.path.exists(csv_path):
        return

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = _csv.reader(f)
        rows = list(reader)

    imported = 0
    for row in rows[1:]:  # skip header
        if len(row) < 2 or not row[1].strip():
            continue

        status_raw = row[0].strip().lower()
        question = row[1].strip()
        hooks_raw = row[2].strip() if len(row) > 2 else ""
        talking_raw = row[3].strip() if len(row) > 3 else ""

        # Determine status
        if status_raw == "done":
            idea_status = "approved"
        elif status_raw == "duplicate":
            continue  # skip duplicates
        else:
            idea_status = "pending"

        # Parse sort order from the number column
        try:
            sort_order = int(status_raw) if status_raw.isdigit() else 999
        except ValueError:
            sort_order = 999

        # Insert content idea
        cursor = conn.execute(
            "INSERT INTO content_ideas (question, talking_points, status, sort_order) VALUES (?, ?, ?, ?)",
            (question, talking_raw, idea_status, sort_order)
        )
        idea_id = cursor.lastrowid

        # Parse and insert individual hooks
        if hooks_raw:
            parts = re.split(r'\n\s*\d+\.\s+', '\n' + hooks_raw)
            hook_num = 0
            for p in parts:
                p = p.strip()
                if p and len(p) > 10:
                    hook_num += 1
                    # Categorize
                    pl = p.lower()
                    if any(w in pl for w in ["how to", "here's how", "step", "guide", "tip"]):
                        cat = "How-To / Educational"
                    elif any(w in pl for w in ["?", "what if", "did you know", "why"]):
                        cat = "Question / Curiosity"
                    elif any(w in pl for w in ["most people", "nobody", "everyone", "stop", "don't"]):
                        cat = "Contrarian / Bold"
                    elif any(w in pl for w in ["result", "before", "after", "transform", "increase", "%"]):
                        cat = "Results / Proof"
                    elif any(w in pl for w in ["story", "when i", "i remember", "years ago"]):
                        cat = "Storytelling"
                    else:
                        cat = "General"

                    # Score based on position (first hooks tend to be strongest)
                    score = max(90 - (hook_num - 1) * 10, 50)

                    conn.execute("""
                        INSERT INTO hook_ideas (hook_text, category, hook_score, content_idea_id, source_username, status)
                        VALUES (?, ?, ?, ?, 'content-csv', 'pending')
                    """, (p, cat, score, idea_id))

        imported += 1

    conn.commit()
    print(f"Imported {imported} content ideas from CSV")


def _seed_hashtag_library(conn):
    """Populate the hashtag library with curated industry hashtags."""
    existing = conn.execute("SELECT COUNT(*) as c FROM hashtag_library").fetchone()["c"]
    if existing > 0:
        return

    tags = [
        # ── MedSpa & Aesthetics ──
        ("medspa", "MedSpa & Aesthetics", "high", "50M+", "Core industry tag — high volume, broad reach"),
        ("medicalspa", "MedSpa & Aesthetics", "high", "10M+", "Alternative spelling — less saturated than #medspa"),
        ("medspanearme", "MedSpa & Aesthetics", "medium", "1M+", "Local discovery — patients searching for nearby clinics"),
        ("medspalife", "MedSpa & Aesthetics", "medium", "500K+", "Lifestyle angle — behind the scenes, culture"),
        ("medspafacials", "MedSpa & Aesthetics", "medium", "500K+", "Treatment-specific — facial focused content"),
        ("aestheticmedicine", "MedSpa & Aesthetics", "high", "5M+", "Professional positioning — authority content"),
        ("aesthetics", "MedSpa & Aesthetics", "high", "30M+", "Broad aesthetic industry tag"),
        ("aesthetictreatments", "MedSpa & Aesthetics", "medium", "2M+", "Treatment showcase content"),
        ("medicalesthetics", "MedSpa & Aesthetics", "medium", "1M+", "Professional variant — Canadian/UK spelling too"),
        ("aestheticclinic", "MedSpa & Aesthetics", "medium", "3M+", "Clinic branding and showcase"),

        # ── Injectables & Botox ──
        ("botox", "Injectables", "high", "20M+", "Highest volume injectable tag — very competitive"),
        ("botoxbeforeandafter", "Injectables", "high", "5M+", "Transformation content — high engagement"),
        ("fillers", "Injectables", "high", "10M+", "Broad filler content"),
        ("dermalfillers", "Injectables", "high", "8M+", "Professional filler terminology"),
        ("lipfiller", "Injectables", "high", "15M+", "Top trending procedure — huge patient interest"),
        ("lipfillers", "Injectables", "high", "10M+", "Plural variant — also very high volume"),
        ("juvederm", "Injectables", "high", "3M+", "Brand-specific — attracts product-aware patients"),
        ("restylane", "Injectables", "medium", "1M+", "Brand-specific filler"),
        ("dysport", "Injectables", "medium", "1M+", "Botox alternative brand — growing awareness"),
        ("cheekfiller", "Injectables", "medium", "2M+", "Treatment-specific — mid-face content"),
        ("jawlinefiller", "Injectables", "medium", "1M+", "Trending procedure — sculpting content"),
        ("chinfiller", "Injectables", "medium", "500K+", "Niche but growing — profile balancing"),
        ("antiwrinkle", "Injectables", "medium", "2M+", "Patient-friendly term for neuromodulators"),
        ("injectables", "Injectables", "high", "5M+", "Umbrella injectable content"),

        # ── Plastic Surgery ──
        ("plasticsurgery", "Plastic Surgery", "high", "25M+", "Core surgery tag — very high volume"),
        ("plasticsurgeon", "Plastic Surgery", "high", "10M+", "Practitioner positioning"),
        ("cosmeticsurgery", "Plastic Surgery", "high", "8M+", "Elective procedure focus"),
        ("rhinoplasty", "Plastic Surgery", "high", "10M+", "Top searched procedure — nose job content"),
        ("nosejob", "Plastic Surgery", "high", "5M+", "Patient-friendly rhinoplasty term"),
        ("facelift", "Plastic Surgery", "high", "5M+", "Premium procedure — affluent patient reach"),
        ("bbl", "Plastic Surgery", "high", "8M+", "Brazilian butt lift — extremely trending"),
        ("breastaugmentation", "Plastic Surgery", "high", "5M+", "Top procedure by volume"),
        ("tummytuck", "Plastic Surgery", "high", "5M+", "Body contouring — mommy makeover audience"),
        ("mommymakeover", "Plastic Surgery", "medium", "2M+", "Package procedure — strong niche"),
        ("liposuction", "Plastic Surgery", "high", "5M+", "Body sculpting content"),
        ("beforeandafter", "Plastic Surgery", "high", "50M+", "Universal transformation tag — highest engagement driver"),

        # ── Skincare & Treatments ──
        ("skincare", "Skincare & Treatments", "high", "100M+", "Massive volume — use with niche tags"),
        ("skincareroutine", "Skincare & Treatments", "high", "30M+", "Routine/regimen content"),
        ("glowingskin", "Skincare & Treatments", "high", "20M+", "Aspirational results content"),
        ("clearskin", "Skincare & Treatments", "high", "15M+", "Results-focused — acne/texture audience"),
        ("microneedling", "Skincare & Treatments", "high", "5M+", "Trending treatment — collagen induction"),
        ("chemicalpeel", "Skincare & Treatments", "medium", "3M+", "Classic treatment — resurface content"),
        ("hydrafacial", "Skincare & Treatments", "high", "3M+", "Branded treatment — huge patient demand"),
        ("prp", "Skincare & Treatments", "medium", "2M+", "Platelet-rich plasma — vampire facial"),
        ("lasertreatment", "Skincare & Treatments", "medium", "2M+", "Laser resurfacing and hair removal"),
        ("laserhairremoval", "Skincare & Treatments", "high", "5M+", "Top non-injectable service"),
        ("ipl", "Skincare & Treatments", "medium", "1M+", "Intense pulsed light — pigmentation content"),
        ("morpheus8", "Skincare & Treatments", "medium", "1M+", "Trending RF microneedling device"),
        ("coolsculpting", "Skincare & Treatments", "medium", "2M+", "Non-surgical fat reduction"),

        # ── Body Contouring ──
        ("bodycontouring", "Body Contouring", "medium", "3M+", "Non-surgical body shaping umbrella"),
        ("bodysculpting", "Body Contouring", "medium", "2M+", "Sculpting and toning content"),
        ("emsculpt", "Body Contouring", "medium", "500K+", "Muscle toning device — trending"),
        ("nonsurgical", "Body Contouring", "medium", "2M+", "Non-invasive positioning"),
        ("fatreduction", "Body Contouring", "medium", "1M+", "Results-focused body content"),

        # ── Marketing & Business ──
        ("medspacmarketing", "Marketing & Business", "low", "100K+", "Industry-specific marketing content"),
        ("aestheticmarketing", "Marketing & Business", "low", "50K+", "Niche marketing for aesthetics"),
        ("practicemarketing", "Marketing & Business", "low", "50K+", "Medical practice growth content"),
        ("medicalpractice", "Marketing & Business", "medium", "500K+", "Practice management audience"),
        ("patientexperience", "Marketing & Business", "low", "200K+", "Service and experience content"),
        ("socialmediamarketing", "Marketing & Business", "high", "30M+", "Broad SMM — use sparingly"),
        ("digitalmarketing", "Marketing & Business", "high", "50M+", "Broad digital — pair with niche tags"),
        ("healthcaremarketing", "Marketing & Business", "medium", "500K+", "Healthcare-specific marketing"),
        ("medicalaesthetics", "Marketing & Business", "medium", "2M+", "Professional crossover tag"),

        # ── Trending & Lifestyle ──
        ("selfcare", "Trending & Lifestyle", "high", "50M+", "Wellness positioning — broad reach"),
        ("beautytips", "Trending & Lifestyle", "high", "20M+", "Educational beauty content"),
        ("antiaging", "Trending & Lifestyle", "high", "10M+", "Preventative aesthetics audience"),
        ("confidenceboost", "Trending & Lifestyle", "medium", "1M+", "Emotional transformation angle"),
        ("naturalbeauty", "Trending & Lifestyle", "high", "20M+", "Subtle enhancement positioning"),
        ("lookgoodfeelgood", "Trending & Lifestyle", "medium", "2M+", "Aspirational results content"),
        ("beautycommunity", "Trending & Lifestyle", "high", "5M+", "Community building tag"),
        ("skingoals", "Trending & Lifestyle", "medium", "3M+", "Aspirational skin results"),
        ("glowup", "Trending & Lifestyle", "high", "10M+", "Transformation content — viral potential"),
        ("investinyourself", "Trending & Lifestyle", "medium", "2M+", "Premium positioning — affluent audience"),
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO hashtag_library (hashtag, category, popularity, post_volume, description) VALUES (?, ?, ?, ?, ?)",
        tags,
    )
    conn.commit()


# ── Profile CRUD ─────────────────────────────────────────────────────

def add_profile(username, category="Uncategorized"):
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO profiles (username, category) VALUES (?, ?)",
            (username.lower().strip().lstrip("@"), category),
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"Error adding profile {username}: {e}")
        return False
    finally:
        conn.close()


def remove_profile(username):
    conn = get_db()
    try:
        profile = conn.execute(
            "SELECT id FROM profiles WHERE username = ?",
            (username.lower().strip(),),
        ).fetchone()
        if profile:
            conn.execute("DELETE FROM posts WHERE profile_id = ?", (profile["id"],))
            conn.execute("DELETE FROM scrape_log WHERE profile_id = ?", (profile["id"],))
            conn.execute("DELETE FROM profiles WHERE id = ?", (profile["id"],))
            conn.commit()
            return True
        return False
    finally:
        conn.close()


# ── Session builder ──────────────────────────────────────────────────

def _build_session():
    """
    Build a requests.Session with Instagram cookies.
    Reads IG_SESSION_ID from environment (set via Railway Variables or
    the /api/set-session endpoint in the dashboard).
    """
    s = req.Session()
    s.headers.update(_HEADERS)

    session_id = os.environ.get("IG_SESSION_ID", "")
    if session_id:
        s.cookies.set("sessionid", session_id, domain=".instagram.com")
        print("[auth] Using IG_SESSION_ID cookie")
    else:
        print("[auth] WARNING: No IG_SESSION_ID set — scraping will likely fail from cloud IPs.")
    return s


# ── Scraping ─────────────────────────────────────────────────────────

def scrape_profile(username, max_posts=30):
    """
    Scrape a single Instagram profile using the web API.
    Returns dict with profile info and list of posts.
    """
    session = _build_session()
    username = username.lower().strip().lstrip("@")

    # ── Step 1: Fetch profile info via web_profile_info endpoint ──
    profile_url = (
        f"https://www.instagram.com/api/v1/users/web_profile_info/"
        f"?username={username}"
    )
    try:
        resp = session.get(profile_url, timeout=15)
    except Exception as e:
        return {"success": False, "error": f"Network error: {e}"}

    if resp.status_code == 404:
        return {"success": False, "error": f"Profile '{username}' does not exist."}
    if resp.status_code == 401:
        return {"success": False, "error": "Session expired. Please update your session cookie via the Settings page on the dashboard."}
    if resp.status_code == 403:
        return {"success": False, "error": "Instagram blocked the request. Please set or update your session cookie via the Settings page on the dashboard."}
    if resp.status_code != 200:
        return {"success": False, "error": f"Instagram returned status {resp.status_code}. Try updating your session cookie."}

    try:
        data = resp.json()
    except Exception:
        return {"success": False, "error": "Could not parse Instagram response. Try updating your session cookie."}

    user = data.get("data", {}).get("user")
    if not user:
        return {"success": False, "error": f"Profile '{username}' not found or is private."}

    follower_count = user.get("edge_followed_by", {}).get("count", 0)
    profile_data = {
        "username": user.get("username", username),
        "full_name": user.get("full_name", ""),
        "follower_count": follower_count,
        "following_count": user.get("edge_follow", {}).get("count", 0),
        "post_count": user.get("edge_owner_to_timeline_media", {}).get("count", 0),
        "bio": user.get("biography", ""),
        "profile_pic_url": user.get("profile_pic_url_hd", user.get("profile_pic_url", "")),
        "is_verified": user.get("is_verified", False),
    }

    # ── Step 2: Extract posts from the same response ──
    timeline = user.get("edge_owner_to_timeline_media", {})
    edges = timeline.get("edges", [])
    posts = []

    for edge in edges[:max_posts]:
        node = edge.get("node", {})
        likes = node.get("edge_liked_by", {}).get("count", 0)
        comments = node.get("edge_media_to_comment", {}).get("count", 0)
        saves = node.get("edge_saved", {}).get("count", node.get("saves", 0))
        is_video = node.get("is_video", False)
        typename = node.get("__typename", "")

        engagement_rate = 0.0
        if follower_count > 0:
            engagement_rate = round(((likes + comments) / follower_count) * 100, 4)

        caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
        caption = caption_edges[0]["node"]["text"] if caption_edges else ""

        hashtags = re.findall(r"#(\w+)", caption)

        if typename == "GraphSidecar":
            post_type = "carousel"
        elif is_video:
            post_type = "video"
        else:
            post_type = "image"

        posted_ts = node.get("taken_at_timestamp", 0)
        posted_at = datetime.utcfromtimestamp(posted_ts).isoformat() if posted_ts else ""

        posts.append({
            "shortcode": node.get("shortcode", ""),
            "post_url": f"https://www.instagram.com/p/{node.get('shortcode', '')}/",
            "caption": caption,
            "post_type": post_type,
            "likes": likes,
            "comments": comments,
            "saves": saves,
            "video_views": node.get("video_view_count", 0) if is_video else 0,
            "engagement_rate": engagement_rate,
            "posted_at": posted_at,
            "thumbnail_url": node.get("thumbnail_src", node.get("display_url", "")),
            "is_video": is_video,
            "hashtags": json.dumps(hashtags),
        })

    # ── Step 3: If we need more posts, paginate ──
    has_next = timeline.get("page_info", {}).get("has_next_page", False)
    end_cursor = timeline.get("page_info", {}).get("end_cursor", "")
    user_id = user.get("id", "")

    while has_next and len(posts) < max_posts and end_cursor and user_id:
        time.sleep(random.uniform(1.0, 2.5))
        variables = json.dumps({"id": user_id, "first": 12, "after": end_cursor})
        next_url = (
            f"https://www.instagram.com/graphql/query/"
            f"?query_hash=e769aa130647d2571c27c44596cb68bd&variables={variables}"
        )
        try:
            resp2 = session.get(next_url, timeout=15)
            if resp2.status_code != 200:
                break
            page_data = resp2.json()
            media = page_data.get("data", {}).get("user", {}).get("edge_owner_to_timeline_media", {})
            for edge in media.get("edges", []):
                if len(posts) >= max_posts:
                    break
                node = edge.get("node", {})
                likes = node.get("edge_liked_by", {}).get("count", 0)
                comments = node.get("edge_media_to_comment", {}).get("count", 0)
                saves = node.get("edge_saved", {}).get("count", node.get("saves", 0))
                is_video = node.get("is_video", False)
                typename = node.get("__typename", "")
                engagement_rate = 0.0
                if follower_count > 0:
                    engagement_rate = round(((likes + comments) / follower_count) * 100, 4)
                caption_edges = node.get("edge_media_to_caption", {}).get("edges", [])
                caption = caption_edges[0]["node"]["text"] if caption_edges else ""
                hashtags = re.findall(r"#(\w+)", caption)
                if typename == "GraphSidecar":
                    post_type = "carousel"
                elif is_video:
                    post_type = "video"
                else:
                    post_type = "image"
                posted_ts = node.get("taken_at_timestamp", 0)
                posted_at = datetime.utcfromtimestamp(posted_ts).isoformat() if posted_ts else ""
                posts.append({
                    "shortcode": node.get("shortcode", ""),
                    "post_url": f"https://www.instagram.com/p/{node.get('shortcode', '')}/",
                    "caption": caption,
                    "post_type": post_type,
                    "likes": likes,
                    "comments": comments,
                    "saves": saves,
                    "video_views": node.get("video_view_count", 0) if is_video else 0,
                    "engagement_rate": engagement_rate,
                    "posted_at": posted_at,
                    "thumbnail_url": node.get("thumbnail_src", node.get("display_url", "")),
                    "is_video": is_video,
                    "hashtags": json.dumps(hashtags),
                })
            has_next = media.get("page_info", {}).get("has_next_page", False)
            end_cursor = media.get("page_info", {}).get("end_cursor", "")
        except Exception:
            break

    return {"profile": profile_data, "posts": posts, "success": True}


# ── Save results ─────────────────────────────────────────────────────

def save_scrape_results(result):
    """Save scraped data to the database."""
    if not result.get("success"):
        return False
    conn = get_db()
    try:
        profile_data = result["profile"]
        conn.execute("""
            UPDATE profiles SET
                full_name = ?, follower_count = ?, following_count = ?,
                post_count = ?, bio = ?, profile_pic_url = ?,
                is_verified = ?, last_scraped = CURRENT_TIMESTAMP
            WHERE username = ?
        """, (
            profile_data["full_name"], profile_data["follower_count"],
            profile_data["following_count"], profile_data["post_count"],
            profile_data["bio"], profile_data["profile_pic_url"],
            1 if profile_data["is_verified"] else 0,
            profile_data["username"],
        ))
        row = conn.execute(
            "SELECT id FROM profiles WHERE username = ?",
            (profile_data["username"],),
        ).fetchone()
        if not row:
            return False
        profile_id = row["id"]
        for post in result["posts"]:
            conn.execute("""
                INSERT INTO posts (
                    profile_id, shortcode, post_url, caption, post_type,
                    likes, comments, saves, video_views, engagement_rate,
                    posted_at, thumbnail_url, is_video, hashtags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(shortcode) DO UPDATE SET
                    likes = excluded.likes,
                    comments = excluded.comments,
                    saves = excluded.saves,
                    video_views = excluded.video_views,
                    engagement_rate = excluded.engagement_rate,
                    caption = excluded.caption,
                    hashtags = excluded.hashtags,
                    scraped_at = CURRENT_TIMESTAMP
            """, (
                profile_id, post["shortcode"], post["post_url"],
                post["caption"], post["post_type"], post["likes"],
                post["comments"], post.get("saves", 0), post["video_views"],
                post["engagement_rate"], post["posted_at"],
                post["thumbnail_url"], 1 if post["is_video"] else 0,
                post["hashtags"],
            ))
        # Record follower snapshot for growth tracking
        conn.execute("""
            INSERT INTO follower_snapshots (profile_id, follower_count, following_count, post_count)
            VALUES (?, ?, ?, ?)
        """, (
            profile_id, profile_data["follower_count"],
            profile_data["following_count"], profile_data["post_count"],
        ))
        conn.execute(
            "INSERT INTO scrape_log (profile_id, status, message) VALUES (?, 'success', ?)",
            (profile_id, f"Scraped {len(result['posts'])} posts"),
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"Error saving results: {e}")
        conn.execute(
            "INSERT INTO scrape_log (profile_id, status, message) VALUES (NULL, 'error', ?)",
            (str(e),),
        )
        conn.commit()
        return False
    finally:
        conn.close()


def scrape_all_profiles(max_posts=30):
    """Scrape all monitored profiles."""
    conn = get_db()
    profiles = conn.execute(
        "SELECT username FROM profiles ORDER BY last_scraped ASC NULLS FIRST"
    ).fetchall()
    conn.close()
    results = []
    for profile in profiles:
        username = profile["username"]
        print(f"Scraping @{username}...")
        result = scrape_profile(username, max_posts=max_posts)
        if result["success"]:
            save_scrape_results(result)
            results.append({"username": username, "status": "success", "posts": len(result["posts"])})
            print(f"  Scraped {len(result['posts'])} posts")
        else:
            results.append({"username": username, "status": "error", "error": result.get("error", "Unknown error")})
            print(f"  Error: {result.get('error')}")
        time.sleep(random.uniform(2, 5))
    return results


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
