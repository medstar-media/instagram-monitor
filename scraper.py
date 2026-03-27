"""
Instagram Scraper Module
Scrapes public Instagram profiles for post data and engagement metrics.
Uses Instaloader library for reliable data extraction.
"""

import instaloader
import time
import random
import json
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

def _get_db_path():
    """Determine the best location for the database file."""
    # Allow override via environment variable
    env_path = os.environ.get("IG_MONITOR_DB")
    if env_path:
        return env_path
    # Default: same directory as the script
    default = os.path.join(os.path.dirname(os.path.abspath(__file__)), "monitor.db")
    # Test if the directory supports SQLite (some network/mounted dirs don't)
    try:
        import tempfile
        test_path = default + ".test"
        c = sqlite3.connect(test_path)
        c.execute("CREATE TABLE _t (id INTEGER)")
        c.close()
        os.remove(test_path)
        return default
    except Exception:
        # Fallback to home directory
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

        CREATE INDEX IF NOT EXISTS idx_posts_profile ON posts(profile_id);
        CREATE INDEX IF NOT EXISTS idx_posts_engagement ON posts(engagement_rate DESC);
        CREATE INDEX IF NOT EXISTS idx_posts_likes ON posts(likes DESC);
        CREATE INDEX IF NOT EXISTS idx_posts_posted ON posts(posted_at DESC);
    """)

    conn.commit()
    conn.close()


def add_profile(username, category="Uncategorized"):
    """Add a new Instagram profile to monitor."""
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO profiles (username, category) VALUES (?, ?)",
            (username.lower().strip().lstrip("@"), category)
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"Error adding profile {username}: {e}")
        return False
    finally:
        conn.close()


def remove_profile(username):
    """Remove a profile and its posts from monitoring."""
    conn = get_db()
    try:
        profile = conn.execute(
            "SELECT id FROM profiles WHERE username = ?",
            (username.lower().strip(),)
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


def _get_instaloader():
    """
    Create an Instaloader instance, optionally logged in via environment
    variables IG_USERNAME + IG_PASSWORD or IG_SESSION_FILE.
    Logging in avoids 403 blocks from cloud / datacenter IPs.
    """
    L = instaloader.Instaloader(
        download_pictures=False,
        download_videos=False,
        download_video_thumbnails=False,
        download_geotags=False,
        download_comments=False,
        save_metadata=False,
        compress_json=False,
        quiet=True
    )

    # Try session file first (most reliable)
    session_file = os.environ.get("IG_SESSION_FILE")
    if session_file and os.path.exists(session_file):
        try:
            L.load_session_from_file(
                os.environ.get("IG_USERNAME", ""), session_file
            )
            print("[auth] Loaded Instagram session from file")
            return L
        except Exception as e:
            print(f"[auth] Session file load failed: {e}")

    # Try username + password login
    ig_user = os.environ.get("IG_USERNAME")
    ig_pass = os.environ.get("IG_PASSWORD")
    if ig_user and ig_pass:
        try:
            L.login(ig_user, ig_pass)
            print(f"[auth] Logged in to Instagram as {ig_user}")
            return L
        except Exception as e:
            print(f"[auth] Login failed: {e}")

    # Try importing a session cookie directly (sessionid)
    ig_session_id = os.environ.get("IG_SESSION_ID")
    if ig_session_id:
        try:
            import requests
            session = requests.Session()
            session.cookies.set("sessionid", ig_session_id, domain=".instagram.com")
            L.context._session = session
            print("[auth] Loaded Instagram session cookie")
            return L
        except Exception as e:
            print(f"[auth] Session cookie load failed: {e}")

    print("[auth] WARNING: No Instagram credentials set. Scraping may fail from cloud IPs.")
    print("[auth] Set IG_USERNAME + IG_PASSWORD or IG_SESSION_ID in Railway environment variables.")
    return L


def scrape_profile(username, max_posts=30):
    """
    Scrape a single Instagram profile for posts and engagement data.
    Returns dict with profile info and list of posts.
    """
    L = _get_instaloader()

    try:
        profile = instaloader.Profile.from_username(L.context, username)

        profile_data = {
            "username": profile.username,
            "full_name": profile.full_name,
            "follower_count": profile.followers,
            "following_count": profile.followees,
            "post_count": profile.mediacount,
            "bio": profile.biography,
            "profile_pic_url": profile.profile_pic_url,
            "is_verified": profile.is_verified,
        }

        posts = []
        count = 0
        for post in profile.get_posts():
            if count >= max_posts:
                break

            # Calculate engagement rate
            total_engagement = post.likes + post.comments
            engagement_rate = 0.0
            if profile.followers > 0:
                engagement_rate = round((total_engagement / profile.followers) * 100, 4)

            # Extract hashtags from caption
            hashtags = []
            if post.caption_hashtags:
                hashtags = post.caption_hashtags

            post_data = {
                "shortcode": post.shortcode,
                "post_url": f"https://www.instagram.com/p/{post.shortcode}/",
                "caption": post.caption or "",
                "post_type": "video" if post.is_video else ("carousel" if post.typename == "GraphSidecar" else "image"),
                "likes": post.likes,
                "comments": post.comments,
                "video_views": post.video_view_count if post.is_video else 0,
                "engagement_rate": engagement_rate,
                "posted_at": post.date_utc.isoformat(),
                "thumbnail_url": post.url,
                "is_video": post.is_video,
                "hashtags": json.dumps(hashtags),
            }
            posts.append(post_data)
            count += 1

            # Polite delay between post fetches
            time.sleep(random.uniform(0.3, 0.8))

        return {"profile": profile_data, "posts": posts, "success": True}

    except instaloader.exceptions.ProfileNotExistsException:
        return {"success": False, "error": f"Profile '{username}' does not exist."}
    except instaloader.exceptions.LoginRequiredException:
        return {"success": False, "error": "Instagram requires login. Set IG_USERNAME + IG_PASSWORD in Railway environment variables."}
    except instaloader.exceptions.ConnectionException as e:
        err = str(e)
        if "403" in err:
            return {"success": False, "error": "Instagram blocked the request (403). Set IG_USERNAME + IG_PASSWORD in Railway environment variables to fix this."}
        if "401" in err:
            return {"success": False, "error": "Instagram login expired. Update IG_PASSWORD or IG_SESSION_ID in Railway environment variables."}
        return {"success": False, "error": f"Connection error: {err}"}
    except Exception as e:
        return {"success": False, "error": f"Unexpected error: {str(e)}"}


def save_scrape_results(result):
    """Save scraped data to the database."""
    if not result.get("success"):
        return False

    conn = get_db()
    try:
        profile_data = result["profile"]

        # Update profile info
        conn.execute("""
            UPDATE profiles SET
                full_name = ?,
                follower_count = ?,
                following_count = ?,
                post_count = ?,
                bio = ?,
                profile_pic_url = ?,
                is_verified = ?,
                last_scraped = CURRENT_TIMESTAMP
            WHERE username = ?
        """, (
            profile_data["full_name"],
            profile_data["follower_count"],
            profile_data["following_count"],
            profile_data["post_count"],
            profile_data["bio"],
            profile_data["profile_pic_url"],
            1 if profile_data["is_verified"] else 0,
            profile_data["username"]
        ))

        # Get profile ID
        row = conn.execute(
            "SELECT id FROM profiles WHERE username = ?",
            (profile_data["username"],)
        ).fetchone()

        if not row:
            return False

        profile_id = row["id"]

        # Insert or update posts
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
                    scraped_at = CURRENT_TIMESTAMP
            """, (
                profile_id,
                post["shortcode"],
                post["post_url"],
                post["caption"],
                post["post_type"],
                post["likes"],
                post["comments"],
                post["video_views"],
                post["engagement_rate"],
                post["posted_at"],
                post["thumbnail_url"],
                1 if post["is_video"] else 0,
                post["hashtags"],
            ))

        # Log successful scrape
        conn.execute(
            "INSERT INTO scrape_log (profile_id, status, message) VALUES (?, 'success', ?)",
            (profile_id, f"Scraped {len(result['posts'])} posts")
        )

        conn.commit()
        return True

    except Exception as e:
        print(f"Error saving results: {e}")
        conn.execute(
            "INSERT INTO scrape_log (profile_id, status, message) VALUES (NULL, 'error', ?)",
            (str(e),)
        )
        conn.commit()
        return False
    finally:
        conn.close()


def scrape_all_profiles(max_posts=30):
    """Scrape all monitored profiles."""
    conn = get_db()
    profiles = conn.execute("SELECT username FROM profiles ORDER BY last_scraped ASC NULLS FIRST").fetchall()
    conn.close()

    results = []
    for profile in profiles:
        username = profile["username"]
        print(f"Scraping @{username}...")
        result = scrape_profile(username, max_posts=max_posts)

        if result["success"]:
            save_scrape_results(result)
            results.append({"username": username, "status": "success", "posts": len(result["posts"])})
            print(f"  ✓ Scraped {len(result['posts'])} posts")
        else:
            results.append({"username": username, "status": "error", "error": result.get("error", "Unknown error")})
            print(f"  ✗ Error: {result.get('error')}")

        # Polite delay between profiles to avoid rate limiting
        time.sleep(random.uniform(2, 5))

    return results


if __name__ == "__main__":
    init_db()
    print("Database initialized.")
    print("Use 'from scraper import *' to access scraping functions.")
