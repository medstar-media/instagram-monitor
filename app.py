"""
MedStar Instagram Monitor — Flask Application
A web-based dashboard for monitoring Instagram influencer post performance.
"""

import os
import json
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, redirect, url_for
from scraper import init_db, get_db, add_profile, remove_profile, scrape_profile, save_scrape_results, scrape_all_profiles

app = Flask(__name__)
app.secret_key = os.urandom(24)

# Initialize database on startup
init_db()

# Track background scraping status
scrape_status = {"running": False, "message": "", "progress": 0, "total": 0}


# ─── API ROUTES ────────────────────────────────────────────────


@app.route("/")
def dashboard():
    """Main dashboard page."""
    return render_template("dashboard.html")


@app.route("/api/profiles", methods=["GET"])
def get_profiles():
    """Get all monitored profiles."""
    conn = get_db()
    profiles = conn.execute("""
        SELECT p.*,
            COUNT(DISTINCT po.id) as total_posts_scraped,
            COALESCE(AVG(po.engagement_rate), 0) as avg_engagement,
            COALESCE(MAX(po.likes), 0) as max_likes
        FROM profiles p
        LEFT JOIN posts po ON p.id = po.profile_id
        GROUP BY p.id
        ORDER BY p.username
    """).fetchall()
    conn.close()
    return jsonify([dict(p) for p in profiles])


@app.route("/api/profiles", methods=["POST"])
def api_add_profile():
    """Add a new profile to monitor."""
    data = request.json
    username = data.get("username", "").strip().lstrip("@")
    category = data.get("category", "Uncategorized")

    if not username:
        return jsonify({"error": "Username is required"}), 400

    success = add_profile(username, category)
    if success:
        return jsonify({"message": f"@{username} added successfully", "username": username})
    return jsonify({"error": f"Could not add @{username}"}), 400


@app.route("/api/profiles/<username>", methods=["DELETE"])
def api_remove_profile(username):
    """Remove a profile from monitoring."""
    success = remove_profile(username)
    if success:
        return jsonify({"message": f"@{username} removed"})
    return jsonify({"error": "Profile not found"}), 404


@app.route("/api/profiles/<username>/scrape", methods=["POST"])
def api_scrape_profile(username):
    """Scrape a single profile."""
    max_posts = request.json.get("max_posts", 30) if request.json else 30

    result = scrape_profile(username, max_posts=max_posts)
    if result["success"]:
        save_scrape_results(result)
        return jsonify({
            "message": f"Scraped {len(result['posts'])} posts from @{username}",
            "posts_count": len(result["posts"])
        })
    return jsonify({"error": result.get("error", "Scrape failed")}), 400


@app.route("/api/scrape-all", methods=["POST"])
def api_scrape_all():
    """Start scraping all profiles in background."""
    global scrape_status

    if scrape_status["running"]:
        return jsonify({"error": "A scrape is already running"}), 409

    max_posts = request.json.get("max_posts", 30) if request.json else 30

    def run_scrape():
        global scrape_status
        scrape_status = {"running": True, "message": "Starting...", "progress": 0, "total": 0}
        try:
            conn = get_db()
            profiles = conn.execute("SELECT username FROM profiles ORDER BY last_scraped ASC NULLS FIRST").fetchall()
            conn.close()

            scrape_status["total"] = len(profiles)

            for i, profile in enumerate(profiles):
                username = profile["username"]
                scrape_status["message"] = f"Scraping @{username}..."
                scrape_status["progress"] = i

                result = scrape_profile(username, max_posts=max_posts)
                if result["success"]:
                    save_scrape_results(result)

            scrape_status["message"] = "Complete!"
            scrape_status["progress"] = scrape_status["total"]
        except Exception as e:
            scrape_status["message"] = f"Error: {str(e)}"
        finally:
            scrape_status["running"] = False

    thread = threading.Thread(target=run_scrape, daemon=True)
    thread.start()
    return jsonify({"message": "Scraping started in background"})


@app.route("/api/scrape-status")
def api_scrape_status():
    """Check background scrape status."""
    return jsonify(scrape_status)


@app.route("/api/posts", methods=["GET"])
def get_posts():
    """
    Get posts with filtering and sorting.
    Query params: sort, order, profile, type, search, limit, offset, days
    """
    sort = request.args.get("sort", "engagement_rate")
    order = request.args.get("order", "DESC")
    profile_filter = request.args.get("profile", "")
    post_type = request.args.get("type", "")
    search = request.args.get("search", "")
    limit = min(int(request.args.get("limit", 50)), 200)
    offset = int(request.args.get("offset", 0))
    days = request.args.get("days", "")

    # Whitelist sortable columns
    allowed_sorts = {
        "engagement_rate", "likes", "comments", "video_views",
        "posted_at", "scraped_at"
    }
    if sort not in allowed_sorts:
        sort = "engagement_rate"
    if order not in ("ASC", "DESC"):
        order = "DESC"

    query = """
        SELECT po.*, pr.username, pr.full_name, pr.follower_count, pr.profile_pic_url,
               pr.category, pr.is_verified
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        WHERE 1=1
    """
    params = []

    if profile_filter:
        query += " AND pr.username = ?"
        params.append(profile_filter)

    if post_type:
        query += " AND po.post_type = ?"
        params.append(post_type)

    if search:
        query += " AND (po.caption LIKE ? OR po.hashtags LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    if days:
        query += " AND po.posted_at >= datetime('now', ?)"
        params.append(f"-{days} days")

    query += f" ORDER BY po.{sort} {order} LIMIT ? OFFSET ?"
    params.extend([limit, offset])

    conn = get_db()
    posts = conn.execute(query, params).fetchall()

    # Get total count for pagination
    count_query = """
        SELECT COUNT(*) as total FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id WHERE 1=1
    """
    count_params = []
    if profile_filter:
        count_query += " AND pr.username = ?"
        count_params.append(profile_filter)
    if post_type:
        count_query += " AND po.post_type = ?"
        count_params.append(post_type)
    if search:
        count_query += " AND (po.caption LIKE ? OR po.hashtags LIKE ?)"
        count_params.extend([f"%{search}%", f"%{search}%"])
    if days:
        count_query += " AND po.posted_at >= datetime('now', ?)"
        count_params.append(f"-{days} days")

    total = conn.execute(count_query, count_params).fetchone()["total"]
    conn.close()

    return jsonify({
        "posts": [dict(p) for p in posts],
        "total": total,
        "limit": limit,
        "offset": offset
    })


@app.route("/api/top-posts", methods=["GET"])
def get_top_posts():
    """Get top performing posts across all profiles."""
    metric = request.args.get("metric", "engagement_rate")
    limit = min(int(request.args.get("limit", 20)), 100)
    days = request.args.get("days", "30")

    allowed_metrics = {"engagement_rate", "likes", "comments", "video_views"}
    if metric not in allowed_metrics:
        metric = "engagement_rate"

    conn = get_db()
    posts = conn.execute(f"""
        SELECT po.*, pr.username, pr.full_name, pr.follower_count,
               pr.profile_pic_url, pr.category, pr.is_verified
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        WHERE po.posted_at >= datetime('now', ?)
        ORDER BY po.{metric} DESC
        LIMIT ?
    """, (f"-{days} days", limit)).fetchall()
    conn.close()

    return jsonify([dict(p) for p in posts])


@app.route("/api/stats", methods=["GET"])
def get_stats():
    """Get overall dashboard statistics."""
    conn = get_db()

    stats = {}

    # Total profiles
    stats["total_profiles"] = conn.execute("SELECT COUNT(*) as c FROM profiles").fetchone()["c"]

    # Total posts tracked
    stats["total_posts"] = conn.execute("SELECT COUNT(*) as c FROM posts").fetchone()["c"]

    # Average engagement rate
    row = conn.execute("SELECT AVG(engagement_rate) as avg_er FROM posts").fetchone()
    stats["avg_engagement_rate"] = round(row["avg_er"] or 0, 4)

    # Top post overall
    top = conn.execute("""
        SELECT po.*, pr.username FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        ORDER BY po.engagement_rate DESC LIMIT 1
    """).fetchone()
    stats["top_post"] = dict(top) if top else None

    # Posts by type
    types = conn.execute("""
        SELECT post_type, COUNT(*) as count FROM posts GROUP BY post_type
    """).fetchall()
    stats["post_types"] = {t["post_type"]: t["count"] for t in types}

    # Engagement by profile (for charts)
    profiles_stats = conn.execute("""
        SELECT pr.username,
               AVG(po.engagement_rate) as avg_engagement,
               SUM(po.likes) as total_likes,
               SUM(po.comments) as total_comments,
               COUNT(po.id) as post_count
        FROM profiles pr
        LEFT JOIN posts po ON pr.id = po.profile_id
        GROUP BY pr.id
        ORDER BY avg_engagement DESC
    """).fetchall()
    stats["profiles_stats"] = [dict(p) for p in profiles_stats]

    # Top hooks (most common first words/phrases in high-performing captions)
    hooks = conn.execute("""
        SELECT caption, engagement_rate, likes, comments
        FROM posts
        WHERE caption IS NOT NULL AND caption != ''
        ORDER BY engagement_rate DESC
        LIMIT 50
    """).fetchall()
    stats["top_hooks"] = [dict(h) for h in hooks]

    # Recent scrape activity
    logs = conn.execute("""
        SELECT sl.*, pr.username FROM scrape_log sl
        LEFT JOIN profiles pr ON sl.profile_id = pr.id
        ORDER BY sl.scraped_at DESC LIMIT 20
    """).fetchall()
    stats["recent_activity"] = [dict(l) for l in logs]

    conn.close()
    return jsonify(stats)


@app.route("/api/hooks", methods=["GET"])
def get_hooks():
    """Analyze top-performing post hooks (opening lines of captions)."""
    limit = min(int(request.args.get("limit", 30)), 100)
    days = request.args.get("days", "30")

    conn = get_db()
    posts = conn.execute("""
        SELECT po.caption, po.engagement_rate, po.likes, po.comments,
               po.post_url, po.post_type, pr.username, pr.follower_count
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        WHERE po.caption IS NOT NULL AND po.caption != ''
          AND po.posted_at >= datetime('now', ?)
        ORDER BY po.engagement_rate DESC
        LIMIT ?
    """, (f"-{days} days", limit)).fetchall()
    conn.close()

    hooks = []
    for post in posts:
        caption = post["caption"]
        # Extract the first sentence/line as the "hook"
        hook = caption.split("\n")[0].strip()
        if len(hook) > 150:
            hook = hook[:150] + "..."

        hooks.append({
            "hook": hook,
            "full_caption": caption,
            "engagement_rate": post["engagement_rate"],
            "likes": post["likes"],
            "comments": post["comments"],
            "post_url": post["post_url"],
            "post_type": post["post_type"],
            "username": post["username"],
            "follower_count": post["follower_count"]
        })

    return jsonify(hooks)


@app.route("/api/export", methods=["GET"])
def export_data():
    """Export all post data as JSON."""
    conn = get_db()
    posts = conn.execute("""
        SELECT po.*, pr.username, pr.full_name, pr.follower_count, pr.category
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        ORDER BY po.engagement_rate DESC
    """).fetchall()
    conn.close()

    return jsonify({
        "exported_at": datetime.now().isoformat(),
        "total_posts": len(posts),
        "posts": [dict(p) for p in posts]
    })


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  MedStar Instagram Monitor")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60 + "\n")
    app.run(debug=True, host="0.0.0.0", port=5000)
