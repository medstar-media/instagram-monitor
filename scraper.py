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
        CREATE INDEX IF NOT EXISTS idx_snapshots_profile ON follower_snapshots(profile_id);
        CREATE INDEX IF NOT EXISTS idx_snapshots_date ON follower_snapshots(recorded_at);
    """)
    conn.commit()
    conn.close()


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
                    likes, comments, video_views, engagement_rate,
                    posted_at, thumbnail_url, is_video, hashtags
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(shortcode) DO UPDATE SET
                    likes = excluded.likes,
                    comments = excluded.comments,
                    video_views = excluded.video_views,
                    engagement_rate = excluded.engagement_rate,
                    caption = excluded.caption,
                    hashtags = excluded.hashtags,
                    scraped_at = CURRENT_TIMESTAMP
            """, (
                profile_id, post["shortcode"], post["post_url"],
                post["caption"], post["post_type"], post["likes"],
                post["comments"], post["video_views"],
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
