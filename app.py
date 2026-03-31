"""
MedStar Instagram Monitor — Flask Application
A web-based dashboard for monitoring Instagram influencer post performance.
"""

import os
import json
import random
import smtplib
import threading
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from functools import wraps
from flask import Flask, render_template, request, jsonify, redirect, url_for, make_response, session
from werkzeug.security import generate_password_hash, check_password_hash
from scraper import init_db, get_db, add_profile, remove_profile, scrape_profile, save_scrape_results, scrape_all_profiles

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "medstar-default-secret-change-me-in-production")
app.permanent_session_lifetime = timedelta(days=7)

# ─── Email config (set these env vars on Railway) ───
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASS = os.environ.get("SMTP_PASS", "")
SMTP_FROM = os.environ.get("SMTP_FROM", SMTP_USER)


# ─── Auth helpers ───────────────────────────────────────────────

def login_required(f):
    """Decorator to protect routes — redirects to login if not authenticated."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            if request.is_json or request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required"}), 401
            return redirect("/auth/login")
        return f(*args, **kwargs)
    return decorated


def admin_required(f):
    """Decorator to protect admin-only routes."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("user_id"):
            return redirect("/auth/login")
        if not session.get("is_admin"):
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


def generate_2fa_code():
    """Generate a 6-digit numeric code."""
    return f"{random.randint(0, 999999):06d}"


def send_2fa_email(to_email, code, display_name):
    """Send a 2FA verification code via email."""
    if not SMTP_USER or not SMTP_PASS:
        print(f"[2FA] SMTP not configured — code for {to_email}: {code}")
        return True  # Allow login in dev mode without email

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"Your Medstar Media verification code: {code}"
    msg["From"] = SMTP_FROM
    msg["To"] = to_email

    html = f"""
    <div style="font-family:-apple-system,sans-serif;max-width:480px;margin:0 auto;padding:40px 20px;">
        <h2 style="color:#6c5ce7;margin-bottom:8px;">Medstar Media Social</h2>
        <p style="color:#555;font-size:15px;">Hi {display_name},</p>
        <p style="color:#555;font-size:15px;">Your verification code is:</p>
        <div style="background:#f8f9fa;border-radius:12px;padding:24px;text-align:center;margin:24px 0;">
            <span style="font-size:36px;font-weight:700;letter-spacing:8px;color:#333;">{code}</span>
        </div>
        <p style="color:#888;font-size:13px;">This code expires in 10 minutes. If you didn't request this, you can safely ignore this email.</p>
    </div>
    """
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        return True
    except Exception as e:
        print(f"[2FA] Email send error: {e}")
        return False


def mask_email(email):
    """Mask email for display: f***y@medstarmedia.com"""
    parts = email.split("@")
    name = parts[0]
    if len(name) <= 2:
        masked = name[0] + "***"
    else:
        masked = name[0] + "***" + name[-1]
    return f"{masked}@{parts[1]}"


def ensure_default_admin():
    """Create or update the default admin account."""
    conn = get_db()
    default_email = os.environ.get("ADMIN_EMAIL", "support@medstarmedia.com")
    default_pass = os.environ.get("ADMIN_PASSWORD", "medstar2024!")

    count = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    if count == 0:
        conn.execute(
            "INSERT INTO users (email, display_name, password_hash, is_admin) VALUES (?, ?, ?, 1)",
            (default_email, "Medstar Admin", generate_password_hash(default_pass))
        )
        conn.commit()
        print(f"[Auth] Default admin created: {default_email}")
    else:
        # Migrate old default admin email if it exists
        old = conn.execute("SELECT id FROM users WHERE email = 'admin@medstarmedia.com'").fetchone()
        if old and default_email != "admin@medstarmedia.com":
            conn.execute("UPDATE users SET email = ?, display_name = ? WHERE id = ?",
                         (default_email, "Medstar Admin", old["id"]))
            conn.commit()
            print(f"[Auth] Migrated admin email to: {default_email}")
    conn.close()


@app.before_request
def require_login():
    """Protect all routes except auth endpoints and browser-scrape."""
    allowed_prefixes = ("/auth/", "/static/", "/api/browser-scrape")
    if any(request.path.startswith(p) for p in allowed_prefixes):
        return None
    if request.path == "/bookmarklet":
        return None  # Bookmarklet page needs to be accessible
    if request.method == "OPTIONS":
        return None  # CORS preflight
    if not session.get("user_id"):
        if request.is_json or request.path.startswith("/api/"):
            return jsonify({"error": "Authentication required"}), 401
        return redirect("/auth/login")


@app.after_request
def add_cors_headers(response):
    """Allow cross-origin requests from Instagram for browser-assisted scraping."""
    origin = request.headers.get("Origin", "")
    if origin in ("https://www.instagram.com", "https://instagram.com"):
        response.headers["Access-Control-Allow-Origin"] = origin
        response.headers["Access-Control-Allow-Methods"] = "POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response

# Initialize database on startup
init_db()
ensure_default_admin()

# Track background scraping status
scrape_status = {"running": False, "message": "", "progress": 0, "total": 0}


# ─── AUTH ROUTES ────────────────────────────────────────────────


@app.route("/auth/login", methods=["GET", "POST"])
def auth_login():
    """Email + Password login (logs in directly)."""
    if session.get("user_id"):
        return redirect("/")

    if request.method == "GET":
        return render_template("login.html", step="login")

    email = request.form.get("email", "").strip().lower()
    password = request.form.get("password", "")

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email = ? AND is_active = 1", (email,)).fetchone()
    conn.close()

    if not user or not check_password_hash(user["password_hash"], password):
        return render_template("login.html", step="login", error="Invalid email or password.", email=email)

    # Log in directly
    conn = get_db()
    conn.execute("UPDATE users SET last_login = ? WHERE id = ?",
                 (datetime.now().isoformat(), user["id"]))
    conn.commit()
    conn.close()

    session.permanent = True
    session["user_id"] = user["id"]
    session["user_email"] = user["email"]
    session["user_name"] = user["display_name"]
    session["is_admin"] = bool(user["is_admin"])

    return redirect("/")


@app.route("/auth/verify-2fa", methods=["POST"])
def auth_verify_2fa():
    """Step 2: Verify 2FA code."""
    user_id = request.form.get("user_id", type=int)
    code = request.form.get("code", "").strip()

    if not user_id or not code:
        return redirect("/auth/login")

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ? AND is_active = 1", (user_id,)).fetchone()

    if not user:
        conn.close()
        return redirect("/auth/login")

    # Check expiry
    if user["twofa_expires"]:
        expires = datetime.fromisoformat(user["twofa_expires"])
        if datetime.now() > expires:
            conn.close()
            return render_template("login.html", step="twofa",
                                   user_id=user_id, masked_email=mask_email(user["email"]),
                                   error="Code expired. Please request a new one.")

    # Verify code
    if not user["twofa_code"] or not check_password_hash(user["twofa_code"], code):
        conn.close()
        return render_template("login.html", step="twofa",
                               user_id=user_id, masked_email=mask_email(user["email"]),
                               error="Invalid code. Please try again.")

    # Success — clear 2FA code and log in
    conn.execute("UPDATE users SET twofa_code = NULL, twofa_expires = NULL, last_login = ? WHERE id = ?",
                 (datetime.now().isoformat(), user_id))
    conn.commit()
    conn.close()

    session.permanent = True
    session["user_id"] = user["id"]
    session["user_email"] = user["email"]
    session["user_name"] = user["display_name"]
    session["is_admin"] = bool(user["is_admin"])

    return redirect("/")


@app.route("/auth/resend-2fa", methods=["POST"])
def auth_resend_2fa():
    """Resend 2FA code."""
    user_id = request.form.get("user_id", type=int)
    if not user_id:
        return redirect("/auth/login")

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id = ? AND is_active = 1", (user_id,)).fetchone()
    if not user:
        conn.close()
        return redirect("/auth/login")

    code = generate_2fa_code()
    expires = datetime.now() + timedelta(minutes=10)
    conn.execute("UPDATE users SET twofa_code = ?, twofa_expires = ? WHERE id = ?",
                 (generate_password_hash(code), expires.isoformat(), user_id))
    conn.commit()
    conn.close()

    send_2fa_email(user["email"], code, user["display_name"])

    return render_template("login.html", step="twofa",
                           user_id=user_id, masked_email=mask_email(user["email"]),
                           success="New code sent! Check your email.")


@app.route("/auth/logout")
def auth_logout():
    """Log out and clear session."""
    session.clear()
    return redirect("/auth/login")


# ─── USER MANAGEMENT (Admin only) ──────────────────────────────


@app.route("/api/users", methods=["GET"])
@admin_required
def get_users():
    """List all users (admin only)."""
    conn = get_db()
    users = conn.execute("SELECT id, email, display_name, is_admin, is_active, created_at, last_login FROM users ORDER BY created_at").fetchall()
    conn.close()
    return jsonify([dict(u) for u in users])


@app.route("/api/users", methods=["POST"])
@admin_required
def create_user():
    """Create/invite a new user (admin only)."""
    data = request.json
    email = data.get("email", "").strip().lower()
    display_name = data.get("display_name", "").strip()
    password = data.get("password", "").strip()
    is_admin = data.get("is_admin", False)

    if not email or not password or not display_name:
        return jsonify({"error": "Email, name, and password are required"}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400

    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        return jsonify({"error": "A user with this email already exists"}), 409

    conn.execute(
        "INSERT INTO users (email, display_name, password_hash, is_admin) VALUES (?, ?, ?, ?)",
        (email, display_name, generate_password_hash(password), 1 if is_admin else 0)
    )
    conn.commit()
    conn.close()
    return jsonify({"message": f"User {email} created successfully"})


@app.route("/api/users/<int:user_id>", methods=["DELETE"])
@admin_required
def delete_user(user_id):
    """Deactivate a user (admin only)."""
    if user_id == session.get("user_id"):
        return jsonify({"error": "Cannot deactivate yourself"}), 400
    conn = get_db()
    conn.execute("UPDATE users SET is_active = 0 WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "User deactivated"})


@app.route("/api/users/<int:user_id>/reset-password", methods=["POST"])
@admin_required
def reset_user_password(user_id):
    """Reset a user's password (admin only)."""
    data = request.json
    new_password = data.get("password", "").strip()
    if not new_password or len(new_password) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    conn = get_db()
    conn.execute("UPDATE users SET password_hash = ? WHERE id = ?",
                 (generate_password_hash(new_password), user_id))
    conn.commit()
    conn.close()
    return jsonify({"message": "Password reset successfully"})


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

    # Total saves
    saves_row = conn.execute("SELECT SUM(saves) as total_saves, AVG(saves) as avg_saves FROM posts WHERE saves > 0").fetchone()
    stats["total_saves"] = saves_row["total_saves"] or 0
    stats["avg_saves"] = round(saves_row["avg_saves"] or 0, 1)

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
               SUM(po.saves) as total_saves,
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


@app.route("/api/hashtag-leaderboard", methods=["GET"])
def get_hashtag_leaderboard():
    """Rank hashtags by average engagement rate."""
    import re as _re
    days = request.args.get("days", "90")
    min_uses = int(request.args.get("min_uses", 2))

    conn = get_db()
    posts = conn.execute("""
        SELECT po.hashtags, po.caption, po.engagement_rate, po.likes, po.comments
        FROM posts po
        WHERE po.caption IS NOT NULL AND po.caption != ''
          AND po.posted_at >= datetime('now', ?)
    """, (f"-{days} days",)).fetchall()
    conn.close()

    # Aggregate hashtag stats — extract from caption as primary source
    tag_stats = {}
    for post in posts:
        # Try stored hashtags first, fall back to extracting from caption
        tags = []
        try:
            stored = json.loads(post["hashtags"] or "[]")
            if stored:
                tags = stored
        except Exception:
            pass
        if not tags and post["caption"]:
            tags = _re.findall(r"#(\w+)", post["caption"])
        if not tags:
            continue
        for tag in tags:
            tag = tag.lower()
            if tag not in tag_stats:
                tag_stats[tag] = {"uses": 0, "total_er": 0, "total_likes": 0, "total_comments": 0}
            tag_stats[tag]["uses"] += 1
            tag_stats[tag]["total_er"] += post["engagement_rate"]
            tag_stats[tag]["total_likes"] += post["likes"]
            tag_stats[tag]["total_comments"] += post["comments"]

    leaderboard = []
    for tag, s in tag_stats.items():
        if s["uses"] >= min_uses:
            leaderboard.append({
                "hashtag": tag,
                "uses": s["uses"],
                "avg_engagement": round(s["total_er"] / s["uses"], 4),
                "avg_likes": round(s["total_likes"] / s["uses"]),
                "avg_comments": round(s["total_comments"] / s["uses"]),
                "total_likes": s["total_likes"],
            })

    leaderboard.sort(key=lambda x: x["avg_engagement"], reverse=True)
    return jsonify(leaderboard[:50])


@app.route("/api/industry-hashtags", methods=["GET"])
def get_industry_hashtags():
    """Get curated industry hashtags from the library."""
    category = request.args.get("category", "")
    popularity = request.args.get("popularity", "")
    search = request.args.get("search", "")

    query = "SELECT * FROM hashtag_library WHERE 1=1"
    params = []

    if category:
        query += " AND category = ?"
        params.append(category)
    if popularity:
        query += " AND popularity = ?"
        params.append(popularity)
    if search:
        query += " AND (hashtag LIKE ? OR description LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])

    query += " ORDER BY CASE popularity WHEN 'high' THEN 1 WHEN 'medium' THEN 2 WHEN 'low' THEN 3 END, hashtag"

    conn = get_db()
    tags = conn.execute(query, params).fetchall()

    # Get categories for filter
    categories = conn.execute("SELECT DISTINCT category FROM hashtag_library ORDER BY category").fetchall()
    conn.close()

    return jsonify({
        "hashtags": [dict(t) for t in tags],
        "categories": [c["category"] for c in categories],
        "total": len(tags)
    })


@app.route("/api/industry-hashtags", methods=["POST"])
def add_industry_hashtag():
    """Add a custom hashtag to the library."""
    data = request.json
    hashtag = data.get("hashtag", "").strip().lstrip("#").lower()
    category = data.get("category", "Custom")
    description = data.get("description", "")

    if not hashtag:
        return jsonify({"error": "Hashtag is required"}), 400

    conn = get_db()
    try:
        conn.execute(
            "INSERT OR IGNORE INTO hashtag_library (hashtag, category, popularity, post_volume, description) VALUES (?, ?, 'medium', 'Custom', ?)",
            (hashtag, category, description),
        )
        conn.commit()
        return jsonify({"message": f"#{hashtag} added to library"})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    finally:
        conn.close()


@app.route("/api/industry-hashtags/<hashtag>", methods=["DELETE"])
def delete_industry_hashtag(hashtag):
    """Remove a hashtag from the library."""
    conn = get_db()
    conn.execute("DELETE FROM hashtag_library WHERE hashtag = ?", (hashtag.lower(),))
    conn.commit()
    conn.close()
    return jsonify({"message": f"#{hashtag} removed"})


@app.route("/api/posting-times", methods=["GET"])
def get_posting_times():
    """Return posting time data for heatmap (day of week x hour)."""
    days = request.args.get("days", "365")
    profile_filter = request.args.get("profile", "")

    query = """
        SELECT po.posted_at, po.engagement_rate, po.likes, po.comments
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        WHERE po.posted_at IS NOT NULL AND po.posted_at != ''
          AND po.posted_at >= datetime('now', ?)
    """
    params = [f"-{days} days"]
    if profile_filter:
        query += " AND pr.username = ?"
        params.append(profile_filter)

    conn = get_db()
    posts = conn.execute(query, params).fetchall()
    conn.close()

    # Build 7x24 grid (day_of_week x hour)
    grid = [[{"count": 0, "total_er": 0} for _ in range(24)] for _ in range(7)]

    for post in posts:
        try:
            dt = datetime.fromisoformat(post["posted_at"].replace("Z", "+00:00"))
            dow = dt.weekday()  # 0=Monday
            hour = dt.hour
            grid[dow][hour]["count"] += 1
            grid[dow][hour]["total_er"] += post["engagement_rate"]
        except Exception:
            continue

    # Convert to avg engagement
    heatmap = []
    for dow in range(7):
        for hour in range(24):
            cell = grid[dow][hour]
            heatmap.append({
                "day": dow,
                "hour": hour,
                "count": cell["count"],
                "avg_engagement": round(cell["total_er"] / cell["count"], 4) if cell["count"] > 0 else 0
            })

    return jsonify(heatmap)


@app.route("/api/growth", methods=["GET"])
def get_growth():
    """Return follower growth data over time."""
    days = request.args.get("days", "90")
    profile_filter = request.args.get("profile", "")

    query = """
        SELECT fs.follower_count, fs.following_count, fs.post_count,
               fs.recorded_at, pr.username
        FROM follower_snapshots fs
        JOIN profiles pr ON fs.profile_id = pr.id
        WHERE fs.recorded_at >= datetime('now', ?)
    """
    params = [f"-{days} days"]
    if profile_filter:
        query += " AND pr.username = ?"
        params.append(profile_filter)
    query += " ORDER BY fs.recorded_at ASC"

    conn = get_db()
    snapshots = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify([dict(s) for s in snapshots])


@app.route("/api/viral-posts", methods=["GET"])
def get_viral_posts():
    """Find posts with engagement >= 2x the profile's average."""
    days = request.args.get("days", "30")
    multiplier = float(request.args.get("multiplier", 2.0))

    conn = get_db()
    posts = conn.execute("""
        SELECT po.*, pr.username, pr.full_name, pr.follower_count,
               pr.profile_pic_url, pr.is_verified,
               (SELECT AVG(p2.engagement_rate) FROM posts p2 WHERE p2.profile_id = po.profile_id) as profile_avg_er
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        WHERE po.posted_at >= datetime('now', ?)
          AND po.engagement_rate >= ? * (SELECT AVG(p2.engagement_rate) FROM posts p2 WHERE p2.profile_id = po.profile_id)
        ORDER BY po.engagement_rate DESC
        LIMIT 20
    """, (f"-{days} days", multiplier)).fetchall()
    conn.close()

    return jsonify([dict(p) for p in posts])


@app.route("/api/content-ideas", methods=["GET"])
def get_content_ideas():
    """Get all content ideas (topics) with their hooks."""
    status = request.args.get("status", "")
    conn = get_db()

    # Get content ideas
    query = "SELECT * FROM content_ideas"
    params = []
    if status:
        query += " WHERE status = ?"
        params.append(status)
    query += " ORDER BY CASE status WHEN 'approved' THEN 0 WHEN 'pending' THEN 1 WHEN 'declined' THEN 2 END, sort_order, id"
    ideas = conn.execute(query, params).fetchall()

    # Get hooks grouped by content_idea_id
    hooks = conn.execute("SELECT * FROM hook_ideas WHERE content_idea_id IS NOT NULL ORDER BY hook_score DESC, id").fetchall()
    hooks_by_idea = {}
    for h in hooks:
        cid = h["content_idea_id"]
        if cid not in hooks_by_idea:
            hooks_by_idea[cid] = []
        hooks_by_idea[cid].append(dict(h))

    result = []
    for idea in ideas:
        d = dict(idea)
        d["hooks"] = hooks_by_idea.get(idea["id"], [])
        result.append(d)

    conn.close()
    return jsonify(result)


@app.route("/api/content-ideas/<int:idea_id>", methods=["PATCH"])
def update_content_idea(idea_id):
    """Update content idea status: approved, declined, pending."""
    data = request.json
    new_status = data.get("status", "pending")
    if new_status not in ("approved", "declined", "pending"):
        return jsonify({"error": "Invalid status"}), 400
    conn = get_db()
    conn.execute("UPDATE content_ideas SET status = ? WHERE id = ?", (new_status, idea_id))
    conn.commit()
    conn.close()
    return jsonify({"message": "Updated", "id": idea_id, "status": new_status})


@app.route("/api/hook-ideas", methods=["GET"])
def get_hook_ideas():
    """Get hook ideas — standalone ones not linked to content ideas."""
    status = request.args.get("status", "")
    conn = get_db()
    query = "SELECT * FROM hook_ideas WHERE content_idea_id IS NULL"
    params = []
    if status:
        query += " AND status = ?"
        params.append(status)
    query += " ORDER BY hook_score DESC, CASE status WHEN 'liked' THEN 0 WHEN 'pending' THEN 1 WHEN 'dismissed' THEN 2 END, created_at DESC"
    ideas = conn.execute(query, params).fetchall()
    conn.close()
    return jsonify([dict(i) for i in ideas])


@app.route("/api/hook-ideas/generate", methods=["POST"])
def generate_hook_ideas():
    """Generate hook ideas from top-performing posts across all monitored profiles."""
    import re as _re
    conn = get_db()
    posts = conn.execute("""
        SELECT po.caption, po.engagement_rate, po.likes, po.comments,
               po.post_url, po.post_type, pr.username, pr.follower_count
        FROM posts po JOIN profiles pr ON po.profile_id = pr.id
        WHERE po.caption IS NOT NULL AND po.caption != ''
        ORDER BY po.engagement_rate DESC LIMIT 80
    """).fetchall()
    if not posts:
        conn.close()
        return jsonify({"generated": 0, "message": "No posts with captions found."})

    existing = set()
    for row in conn.execute("SELECT hook_text FROM hook_ideas").fetchall():
        existing.add(row["hook_text"].strip().lower())

    generated = 0
    for post in posts:
        caption = post["caption"].strip()
        if not caption:
            continue
        lines = caption.split("\n")
        hook = lines[0].strip()
        if not hook or len(hook) < 10:
            hook = " ".join(l.strip() for l in lines[:2] if l.strip())
        if not hook or len(hook) < 10:
            continue
        if len(hook) > 200:
            hook = hook[:200].rsplit(" ", 1)[0] + "..."
        if hook.lower() in existing:
            continue

        hook_lower = hook.lower()
        if any(w in hook_lower for w in ["how to", "here's how", "step", "guide", "tip"]):
            category = "How-To / Educational"
        elif any(w in hook_lower for w in ["?", "what if", "did you know", "why"]):
            category = "Question / Curiosity"
        elif any(w in hook_lower for w in ["most people", "nobody", "everyone", "stop", "don't"]):
            category = "Contrarian / Bold"
        elif any(w in hook_lower for w in ["result", "before", "after", "transform", "increase", "%"]):
            category = "Results / Proof"
        elif any(w in hook_lower for w in ["story", "when i", "i remember", "years ago", "journey"]):
            category = "Storytelling"
        else:
            category = "General"

        # Score the hook based on engagement performance
        er = post["engagement_rate"] or 0
        score = min(int(er * 100 * 20), 100)  # Scale ER to 0-100

        conn.execute("""
            INSERT INTO hook_ideas (hook_text, category, source_post_url, source_username, source_engagement, hook_score)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (hook, category, post["post_url"], post["username"], er, score))
        existing.add(hook.lower())
        generated += 1

    conn.commit()
    conn.close()
    return jsonify({"generated": generated, "message": f"Generated {generated} new hook ideas from top posts."})


@app.route("/api/hook-ideas/<int:idea_id>", methods=["PATCH"])
def update_hook_idea(idea_id):
    """Update hook idea status or score."""
    data = request.json
    conn = get_db()
    if "status" in data:
        new_status = data["status"]
        if new_status not in ("liked", "dismissed", "pending"):
            conn.close()
            return jsonify({"error": "Invalid status"}), 400
        conn.execute("UPDATE hook_ideas SET status = ? WHERE id = ?", (new_status, idea_id))
    if "hook_score" in data:
        conn.execute("UPDATE hook_ideas SET hook_score = ? WHERE id = ?", (int(data["hook_score"]), idea_id))
    conn.commit()
    conn.close()
    return jsonify({"message": "Updated", "id": idea_id})


@app.route("/api/hook-ideas/<int:idea_id>", methods=["DELETE"])
def delete_hook_idea(idea_id):
    """Permanently delete a hook idea."""
    conn = get_db()
    conn.execute("DELETE FROM hook_ideas WHERE id = ?", (idea_id,))
    conn.commit()
    conn.close()
    return jsonify({"message": "Deleted", "id": idea_id})


@app.route("/api/hook-ideas", methods=["POST"])
def add_custom_hook():
    """Add a custom hook idea manually."""
    data = request.json
    hook_text = (data.get("hook_text") or "").strip()
    category = data.get("category", "Custom")
    if not hook_text:
        return jsonify({"error": "Hook text is required"}), 400
    conn = get_db()
    conn.execute(
        "INSERT INTO hook_ideas (hook_text, category, source_username) VALUES (?, ?, 'manual')",
        (hook_text, category)
    )
    conn.commit()
    conn.close()
    return jsonify({"message": f"Hook added"})


@app.route("/api/ad-recommendations", methods=["GET"])
def get_ad_recommendations():
    """Recommend posts that Medstar Media should boost with paid ads.
    Scores posts on engagement rate, like volume, comment volume,
    video views, recency, and content type — then returns the top candidates."""
    import re as _re
    limit = min(int(request.args.get("limit", 10)), 30)

    conn = get_db()

    # Get medstarmedia's profile
    profile = conn.execute(
        "SELECT * FROM profiles WHERE username = 'medstarmedia'"
    ).fetchone()
    if not profile:
        conn.close()
        return jsonify({"recommendations": [], "message": "Medstar Media profile not found."})

    follower_count = profile["follower_count"] or 1

    # Get all medstarmedia posts
    posts = conn.execute("""
        SELECT po.*, pr.username, pr.follower_count
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        WHERE pr.username = 'medstarmedia'
        ORDER BY po.posted_at DESC
    """).fetchall()

    # Also get profile-wide averages for benchmarking
    avgs = conn.execute("""
        SELECT AVG(engagement_rate) as avg_er,
               AVG(likes) as avg_likes,
               AVG(comments) as avg_comments,
               MAX(engagement_rate) as max_er
        FROM posts po
        JOIN profiles pr ON po.profile_id = pr.id
        WHERE pr.username = 'medstarmedia'
    """).fetchone()
    conn.close()

    if not posts:
        return jsonify({"recommendations": [], "message": "No posts found for @medstarmedia. Scrape the profile first."})

    avg_er = avgs["avg_er"] or 0.01
    avg_likes = avgs["avg_likes"] or 1
    avg_comments = avgs["avg_comments"] or 1
    max_er = avgs["max_er"] or 0.01

    scored_posts = []
    for post in posts:
        p = dict(post)
        er = p["engagement_rate"] or 0
        likes = p["likes"] or 0
        comments = p["comments"] or 0
        saves = p.get("saves") or 0
        views = p["video_views"] or 0
        post_type = p["post_type"] or "image"

        # --- Scoring factors (each 0-1 range, weighted) ---

        # 1. Engagement rate vs average (35% weight) - higher = proven content
        er_score = min(er / max_er, 1.0) if max_er > 0 else 0

        # 2. Like volume (15% weight) - social proof for ads
        like_score = min(likes / (avg_likes * 3), 1.0) if avg_likes > 0 else 0

        # 3. Comment volume (10% weight) - conversation-starting content
        comment_score = min(comments / (avg_comments * 3), 1.0) if avg_comments > 0 else 0

        # 4. Saves (10% weight) - saves signal high-value content worth boosting
        save_score = min(saves / max(likes * 0.1, 1), 1.0) if saves > 0 else 0

        # 5. Video views bonus (10% weight) - video content scales better
        view_score = 0
        if views > 0:
            view_score = min(views / 10000, 1.0)

        # 6. Content type bonus (10% weight) - reels/video outperform in ads
        type_score = 0.9 if post_type == "video" else 0.7 if post_type == "carousel" else 0.4

        # 7. Recency (10% weight) - fresher content performs better in ads
        recency_score = 0.5
        try:
            from datetime import datetime as _dt
            posted = _dt.fromisoformat(p["posted_at"].replace("Z", "+00:00"))
            days_ago = (_dt.now(posted.tzinfo) - posted).days
            recency_score = max(0, 1.0 - (days_ago / 180))
        except Exception:
            pass

        # Weighted total
        total_score = (
            er_score * 0.35 +
            like_score * 0.15 +
            comment_score * 0.10 +
            save_score * 0.10 +
            view_score * 0.10 +
            type_score * 0.10 +
            recency_score * 0.10
        )

        # Generate a reason why this post is recommended
        reasons = []
        if er > avg_er * 1.5:
            reasons.append(f"Engagement rate ({round(er * 100, 2)}%) is {round(er / avg_er, 1)}x above your average")
        if likes > avg_likes * 1.5:
            reasons.append(f"{likes} likes — strong social proof")
        if comments > avg_comments * 2:
            reasons.append(f"{comments} comments — high conversation potential")
        if saves > 0:
            reasons.append(f"{saves} saves — audience wants to revisit this")
        if views > 5000:
            reasons.append(f"{formatNumber_py(views)} views — proven reach")
        if post_type == "video":
            reasons.append("Video/Reel — best ad format for reach")
        elif post_type == "carousel":
            reasons.append("Carousel — great for retargeting & education")
        if recency_score > 0.7:
            reasons.append("Recent post — timely & relevant")
        if not reasons:
            reasons.append("Solid overall metrics for paid promotion")

        # Extract hashtags for context
        caption = p.get("caption", "") or ""
        tags = _re.findall(r"#(\w+)", caption)

        p["ad_score"] = round(total_score * 100)
        p["ad_reasons"] = reasons
        p["hashtags_found"] = tags[:5]
        p["hook"] = caption.split("\n")[0][:120] if caption else ""
        scored_posts.append(p)

    # Sort by score descending
    scored_posts.sort(key=lambda x: x["ad_score"], reverse=True)
    top = scored_posts[:limit]

    return jsonify({
        "recommendations": top,
        "profile": "medstarmedia",
        "total_posts_analyzed": len(posts),
        "avg_engagement": round(avg_er * 100, 2)
    })


@app.route("/api/growth-tips", methods=["GET"])
def get_growth_tips():
    """Generate data-driven growth tips for a profile (defaults to medstarmedia)."""
    import re as _re
    profile_filter = request.args.get("profile", "medstarmedia")

    conn = get_db()

    # Get profile info
    profile = conn.execute(
        "SELECT * FROM profiles WHERE username = ?", (profile_filter,)
    ).fetchone()
    if not profile:
        conn.close()
        return jsonify({"tips": [{"icon": "ℹ️", "title": "No Data", "text": "Profile not found. Scrape it first."}]})

    profile_id = profile["id"]
    follower_count = profile["follower_count"] or 0

    # Get post stats
    posts = conn.execute("""
        SELECT po.engagement_rate, po.likes, po.comments, po.post_type,
               po.caption, po.posted_at, po.video_views
        FROM posts po WHERE po.profile_id = ?
        ORDER BY po.posted_at DESC
    """, (profile_id,)).fetchall()

    # Get growth snapshots
    snapshots = conn.execute("""
        SELECT follower_count, recorded_at FROM follower_snapshots
        WHERE profile_id = ? ORDER BY recorded_at ASC
    """, (profile_id,)).fetchall()

    # Get all profiles for comparison
    all_profiles = conn.execute("""
        SELECT pr.username, pr.follower_count,
               AVG(po.engagement_rate) as avg_er,
               COUNT(po.id) as post_count
        FROM profiles pr
        LEFT JOIN posts po ON pr.id = po.profile_id
        GROUP BY pr.id
    """).fetchall()
    conn.close()

    tips = []

    if not posts:
        return jsonify({"tips": [{"icon": "📊", "title": "No Posts Yet", "text": f"Scrape @{profile_filter} to get growth insights."}]})

    # --- Compute metrics ---
    total_posts = len(posts)
    avg_er = sum(p["engagement_rate"] for p in posts) / total_posts
    avg_likes = sum(p["likes"] for p in posts) / total_posts
    avg_comments = sum(p["comments"] for p in posts) / total_posts
    lc_ratio = round(avg_likes / avg_comments, 1) if avg_comments > 0 else 0

    # Post type breakdown
    type_counts = {}
    type_er = {}
    for p in posts:
        t = p["post_type"] or "unknown"
        type_counts[t] = type_counts.get(t, 0) + 1
        type_er.setdefault(t, []).append(p["engagement_rate"])
    best_type = max(type_er, key=lambda t: sum(type_er[t]) / len(type_er[t])) if type_er else None
    best_type_er = round(sum(type_er[best_type]) / len(type_er[best_type]) * 100, 2) if best_type else 0

    # Posting time analysis
    hour_er = {}
    for p in posts:
        try:
            dt = datetime.fromisoformat(p["posted_at"].replace("Z", "+00:00"))
            h = dt.hour
            hour_er.setdefault(h, []).append(p["engagement_rate"])
        except Exception:
            pass
    best_hour = max(hour_er, key=lambda h: sum(hour_er[h]) / len(hour_er[h])) if hour_er else None

    # Hashtag analysis
    all_tags = {}
    for p in posts:
        tags = _re.findall(r"#(\w+)", p["caption"] or "")
        for tag in tags:
            tag = tag.lower()
            all_tags.setdefault(tag, []).append(p["engagement_rate"])
    top_tags = sorted(all_tags.items(), key=lambda x: sum(x[1]) / len(x[1]), reverse=True)[:5]

    # Caption length analysis
    short_captions = [p for p in posts if len(p["caption"] or "") < 100]
    long_captions = [p for p in posts if len(p["caption"] or "") >= 300]
    short_er = sum(p["engagement_rate"] for p in short_captions) / len(short_captions) if short_captions else 0
    long_er = sum(p["engagement_rate"] for p in long_captions) / len(long_captions) if long_captions else 0

    # Comparison to top performer
    top_profile = max(all_profiles, key=lambda p: p["avg_er"] or 0) if all_profiles else None

    # --- Generate tips ---

    # 1. Overall health
    tips.append({
        "icon": "📊",
        "title": "Engagement Overview",
        "text": f"@{profile_filter} averages {round(avg_er * 100, 2)}% engagement across {total_posts} posts ({round(avg_likes)} likes, {round(avg_comments)} comments per post). "
                + ("Great engagement rate! Keep it up." if avg_er >= 0.03 else
                   "Solid engagement — aim for 3%+ to stand out." if avg_er >= 0.015 else
                   "Engagement is below 1.5%. Focus on hooks, CTAs, and Reels to boost interaction.")
    })

    # 2. Content type recommendation
    if best_type:
        type_label = best_type.replace("_", " ").title()
        tips.append({
            "icon": "🎬",
            "title": f"Double Down on {type_label}",
            "text": f"{type_label} posts average {best_type_er}% engagement — your strongest format. "
                    + ("Consider increasing Reels output to 4-5 per week for maximum reach." if "reel" in best_type.lower() or "video" in best_type.lower()
                       else f"Mix more {type_label} content into your calendar for consistent performance.")
        })

    # 3. Best posting time
    if best_hour is not None:
        period = "AM" if best_hour < 12 else "PM"
        display_hour = best_hour if best_hour <= 12 else best_hour - 12
        if display_hour == 0:
            display_hour = 12
        best_hour_er = round(sum(hour_er[best_hour]) / len(hour_er[best_hour]) * 100, 2)
        tips.append({
            "icon": "⏰",
            "title": f"Best Posting Time: {display_hour} {period}",
            "text": f"Posts around {display_hour} {period} average {best_hour_er}% engagement. Schedule content during this window for maximum visibility. Consistency in posting times trains the algorithm to boost your content."
        })

    # 4. Likes-to-comments ratio insight
    if lc_ratio > 0:
        tips.append({
            "icon": "💬",
            "title": f"Likes-to-Comments Ratio: {lc_ratio}:1",
            "text": ("Your audience is highly engaged in conversation — that's a strong community signal!" if lc_ratio < 15 else
                     "Solid ratio. Add more CTAs like 'Drop a 🔥 if you agree' or ask questions to boost comment rates." if lc_ratio < 30 else
                     "High like-to-comment ratio suggests passive engagement. Use carousel posts with questions, polls in Stories, and strong CTAs to spark more comments.")
        })

    # 5. Caption length tip
    if short_captions and long_captions:
        if long_er > short_er:
            tips.append({
                "icon": "📝",
                "title": "Longer Captions Win",
                "text": f"Posts with 300+ character captions average {round(long_er * 100, 2)}% engagement vs {round(short_er * 100, 2)}% for short ones. Tell stories, share value, and use line breaks for readability."
            })
        else:
            tips.append({
                "icon": "📝",
                "title": "Keep Captions Punchy",
                "text": f"Shorter captions ({round(short_er * 100, 2)}% ER) outperform long ones ({round(long_er * 100, 2)}% ER). Lead with a hook, keep it concise, and put the CTA above the fold."
            })

    # 6. Top hashtag performance
    if top_tags:
        tag_list = ", ".join([f"#{t[0]}" for t in top_tags])
        tips.append({
            "icon": "#️⃣",
            "title": "Top Performing Hashtags",
            "text": f"Your highest-engaging hashtags: {tag_list}. Rotate these with trending tags from the Industry Library to maximize discoverability."
        })

    # 7. Competitive benchmark
    if top_profile and top_profile["username"] != profile_filter:
        tips.append({
            "icon": "🏆",
            "title": "Benchmark vs Top Performer",
            "text": f"@{top_profile['username']} leads with {round((top_profile['avg_er'] or 0) * 100, 2)}% avg engagement and {formatNumber_py(top_profile['follower_count'])} followers. Study their content style, posting cadence, and hooks for inspiration."
        })

    # 8. Follower growth insight
    if len(snapshots) >= 2:
        first = snapshots[0]["follower_count"]
        last = snapshots[-1]["follower_count"]
        diff = last - first
        pct = round((diff / first) * 100, 2) if first > 0 else 0
        direction = "📈" if diff > 0 else "📉" if diff < 0 else "➡️"
        tips.append({
            "icon": direction,
            "title": f"Follower Trend: {'+' if diff >= 0 else ''}{diff} ({pct}%)",
            "text": ("Growth is trending up — keep your current strategy and scale what works." if diff > 0 else
                     "Followers are declining. Audit recent content, increase Reels output, engage with comments within the first hour, and collaborate with peers in the medspa niche." if diff < 0 else
                     "Follower count is stable. To accelerate growth, try collaborations, giveaways, or trending audio Reels.")
        })

    # 9. Medspa-specific growth plays
    tips.append({
        "icon": "🚀",
        "title": "MedSpa Growth Plays",
        "text": "Proven tactics for aesthetic/medspa accounts: Before & After carousels (highest save rate), patient testimonial Reels, 'Day in the Life' Stories, provider Q&A sessions, and seasonal treatment spotlights. Use the Industry Tags library for niche-specific hashtags."
    })

    return jsonify({"tips": tips, "profile": profile_filter})


def formatNumber_py(n):
    """Format large numbers with K/M suffix for tips."""
    if n is None:
        return "0"
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


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


@app.route("/api/browser-scrape", methods=["POST", "OPTIONS"])
def browser_scrape():
    """Accept scraped profile data sent from the user's browser.
    The browser fetches Instagram data (using its own cookies) and POSTs here."""
    if request.method == "OPTIONS":
        return "", 204

    # Support both JSON and form-encoded data (form POST bypasses CORS)
    if request.content_type and "application/json" in request.content_type:
        data = request.json
    elif request.form.get("payload"):
        try:
            data = json.loads(request.form["payload"])
        except Exception:
            return jsonify({"error": "Invalid form payload"}), 400
    else:
        return jsonify({"error": "No data received"}), 400

    if not data:
        return jsonify({"error": "No data received"}), 400
    result = data.get("result")
    if not result or not result.get("success"):
        return jsonify({"error": "Invalid scrape result"}), 400

    # Auto-add profile if it doesn't exist
    username = result["profile"]["username"]
    add_profile(username)

    save_scrape_results(result)
    msg = f"Saved {len(result.get('posts', []))} posts for @{username}"

    # If form submission, redirect back to dashboard
    if request.form.get("payload"):
        return redirect(f"/?scraped={username}&posts={len(result.get('posts', []))}")

    return jsonify({"message": msg})


@app.route("/bookmarklet")
def bookmarklet_page():
    """Page with the scraper bookmarklet for team members."""
    dashboard_url = request.url_root.rstrip("/")
    return render_template("bookmarklet.html", dashboard_url=dashboard_url)


@app.route("/scraper.js")
def scraper_js():
    """Serve the bookmarklet scraper script. Loaded inline by the bookmarklet."""
    dashboard_url = request.url_root.rstrip("/")
    js = f"""
(function(){{
  var DASH='{dashboard_url}';
  var path=window.location.pathname.replace(/\\//g,'');
  if(window.location.hostname!=='www.instagram.com'&&window.location.hostname!=='instagram.com'){{
    alert('Please run this on an Instagram profile page!');return;
  }}
  var username=path.split('/')[0]||path;
  if(!username||username==='explore'||username==='reels'||username==='stories'){{
    alert('Please navigate to an Instagram profile page first (e.g. instagram.com/garyvee)');return;
  }}

  var overlay=document.createElement('div');
  overlay.id='medstar-overlay';
  overlay.style.cssText='position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.85);z-index:999999;display:flex;align-items:center;justify-content:center;font-family:-apple-system,sans-serif;';
  overlay.innerHTML='<div style="background:#1e293b;padding:40px;border-radius:16px;text-align:center;max-width:400px;box-shadow:0 20px 60px rgba(0,0,0,0.5)"><div style="font-size:40px;margin-bottom:16px">📊</div><div id="ms-status" style="color:#e2e8f0;font-size:18px;font-weight:600">Fetching profile...</div><div id="ms-detail" style="color:#94a3b8;font-size:14px;margin-top:8px">@'+username+'</div><div style="margin-top:20px;height:4px;background:#334155;border-radius:2px;overflow:hidden"><div id="ms-bar" style="height:100%;width:10%;background:linear-gradient(90deg,#6366f1,#8b5cf6);transition:width 0.3s;border-radius:2px"></div></div></div>';
  document.body.appendChild(overlay);

  var status=document.getElementById('ms-status');
  var detail=document.getElementById('ms-detail');
  var bar=document.getElementById('ms-bar');

  function fail(msg){{ status.textContent='Error'; detail.textContent=msg; detail.style.color='#f87171'; bar.style.background='#ef4444'; bar.style.width='100%'; setTimeout(function(){{ overlay.remove(); }},4000); }}

  fetch('/api/v1/users/web_profile_info/?username='+username,{{
    headers:{{'X-IG-App-ID':'936619743392459','X-Requested-With':'XMLHttpRequest'}}
  }}).then(function(r){{
    if(r.status===401||r.status===403)throw new Error('Login required - please log into Instagram first');
    if(r.status===404)throw new Error('Profile not found');
    if(!r.ok)throw new Error('Instagram returned '+r.status);
    return r.json();
  }}).then(function(d){{
    var user=d.data.user;
    if(!user)throw new Error('Profile not found or is private');
    bar.style.width='30%';
    status.textContent='Got profile info';
    detail.textContent=user.full_name||'@'+username;

    var fc=user.edge_followed_by?user.edge_followed_by.count:0;
    var profile={{
      username:user.username||username,
      full_name:user.full_name||'',
      follower_count:fc,
      following_count:user.edge_follow?user.edge_follow.count:0,
      post_count:user.edge_owner_to_timeline_media?user.edge_owner_to_timeline_media.count:0,
      bio:user.biography||'',
      profile_pic_url:user.profile_pic_url_hd||user.profile_pic_url||'',
      is_verified:user.is_verified||false
    }};

    var userId=user.id;
    status.textContent='Fetching posts...';
    bar.style.width='50%';

    return fetch('/api/v1/feed/user/'+userId+'/?count=30',{{
      headers:{{'X-IG-App-ID':'936619743392459','X-Requested-With':'XMLHttpRequest'}}
    }}).then(function(r2){{ return r2.json(); }}).then(function(feed){{
      var items=feed.items||[];
      bar.style.width='75%';
      status.textContent='Processing '+items.length+' posts...';

      var posts=items.map(function(item){{
        var likes=item.like_count||0;
        var comments=item.comment_count||0;
        var isVideo=item.media_type===2;
        var isCarousel=item.media_type===8;
        var postType=isCarousel?'carousel':(isVideo?'video':'image');
        var caption=item.caption?item.caption.text:'';
        var hashtags=(caption.match(/#(\\w+)/g)||[]).map(function(h){{ return h.slice(1); }});
        var ts=item.taken_at||0;
        var postedAt=ts?new Date(ts*1000).toISOString():'';
        var er=fc>0?Math.round(((likes+comments)/fc)*10000)/100:0;
        var code=item.code||'';
        var thumb='';
        if(item.image_versions2&&item.image_versions2.candidates&&item.image_versions2.candidates.length){{
          thumb=item.image_versions2.candidates[0].url;
        }}else if(item.carousel_media&&item.carousel_media.length){{
          var first=item.carousel_media[0];
          if(first.image_versions2&&first.image_versions2.candidates)thumb=first.image_versions2.candidates[0].url;
        }}
        return {{shortcode:code,post_url:'https://www.instagram.com/p/'+code+'/',caption:caption,post_type:postType,likes:likes,comments:comments,video_views:isVideo?(item.play_count||item.view_count||0):0,engagement_rate:er,posted_at:postedAt,thumbnail_url:thumb,is_video:isVideo,hashtags:JSON.stringify(hashtags)}};
      }});

      bar.style.width='90%';
      status.textContent='Sending to dashboard...';

      var payload=JSON.stringify({{result:{{success:true,profile:profile,posts:posts}}}});
      var form=document.createElement('form');
      form.method='POST';
      form.action=DASH+'/api/browser-scrape';
      var input=document.createElement('input');
      input.type='hidden';
      input.name='payload';
      input.value=payload;
      form.appendChild(input);
      document.body.appendChild(form);
      form.submit();
    }});
  }}).catch(function(e){{ fail(e.message); }});
}})();
"""
    resp = make_response(js)
    resp.headers["Content-Type"] = "application/javascript"
    resp.headers["Cache-Control"] = "no-cache"
    return resp


@app.route("/api/session", methods=["GET"])
def get_session_status():
    """Check if a session cookie is configured."""
    has_session = bool(os.environ.get("IG_SESSION_ID", ""))
    return jsonify({"has_session": has_session})


@app.route("/api/session", methods=["POST"])
def set_session():
    """Set the Instagram session cookie at runtime."""
    data = request.json
    session_id = data.get("session_id", "").strip()
    if not session_id:
        return jsonify({"error": "session_id is required"}), 400
    os.environ["IG_SESSION_ID"] = session_id
    return jsonify({"message": "Session cookie saved successfully."})


if __name__ == "__main__":
    print("\n" + "=" * 60)
    print("  MedStar Instagram Monitor")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60 + "\n")
    app.run(debug=True, host="0.0.0.0", port=5000)
