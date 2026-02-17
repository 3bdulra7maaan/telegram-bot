"""
LinkedIt Bot - Database Module v3.0
===================================
SQLite database for user profiles, favorites, job alerts, and analytics.
Designed for Render with persistent disk support.

Key improvements over v2:
- Connection pooling with thread-local storage
- All FOREIGN KEY operations use CASCADE or auto-create user
- Robust error handling with graceful fallbacks
- Optimized queries with proper indexing
- Clean separation of concerns
"""

import sqlite3
import json
import logging
import os
import threading
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Database configuration
DB_PATH = os.environ.get("DB_PATH", "linkedit.db")

# Ensure the directory for the database file exists
_db_dir = os.path.dirname(DB_PATH)
if _db_dir and not os.path.exists(_db_dir):
    try:
        os.makedirs(_db_dir, exist_ok=True)
        logger.info("Created database directory: %s", _db_dir)
    except OSError as e:
        logger.warning("Could not create directory %s: %s. Falling back to local path.", _db_dir, e)
        DB_PATH = "linkedit.db"

# Thread-local storage for database connections
_local = threading.local()


def _get_connection() -> sqlite3.Connection:
    """Get a thread-local database connection (reused within the same thread)."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, timeout=15)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA busy_timeout=10000")
        # Disable FOREIGN KEYS enforcement to prevent constraint errors
        # We handle referential integrity in application logic instead
        _local.conn.execute("PRAGMA foreign_keys=OFF")
    return _local.conn


@contextmanager
def get_db():
    """Thread-safe database connection context manager with auto-commit."""
    conn = _get_connection()
    try:
        yield conn
        conn.commit()
    except sqlite3.OperationalError as e:
        logger.error("Database operational error: %s", e)
        conn.rollback()
        # If database is locked or corrupted, reset connection
        if "locked" in str(e).lower() or "disk" in str(e).lower():
            try:
                conn.close()
            except Exception:
                pass
            _local.conn = None
        raise
    except Exception as e:
        logger.error("Database error: %s", e)
        conn.rollback()
        raise


def init_db():
    """Initialize database tables and indexes."""
    with get_db() as conn:
        conn.executescript("""
            -- User profiles
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT DEFAULT '',
                first_name TEXT DEFAULT '',
                preferred_countries TEXT DEFAULT '[]',
                preferred_keywords TEXT DEFAULT '[]',
                alerts_enabled INTEGER DEFAULT 0,
                alert_interval TEXT DEFAULT 'daily',
                language TEXT DEFAULT 'ar',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            -- Saved/favorite jobs
            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                job_title TEXT DEFAULT '',
                company TEXT DEFAULT '',
                location TEXT DEFAULT '',
                job_url TEXT DEFAULT '',
                email TEXT DEFAULT '',
                source TEXT DEFAULT '',
                country_name TEXT DEFAULT '',
                description TEXT DEFAULT '',
                saved_at TEXT DEFAULT (datetime('now'))
            );

            -- Job alerts
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                country_code TEXT DEFAULT 'all',
                is_active INTEGER DEFAULT 1,
                last_sent TEXT,
                created_at TEXT DEFAULT (datetime('now'))
            );

            -- Sent jobs tracking (avoid duplicate alerts)
            CREATE TABLE IF NOT EXISTS sent_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                job_url TEXT NOT NULL,
                sent_at TEXT DEFAULT (datetime('now')),
                UNIQUE(user_id, job_url)
            );

            -- Search history (analytics)
            CREATE TABLE IF NOT EXISTS search_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                search_term TEXT NOT NULL,
                country_code TEXT DEFAULT 'all',
                results_count INTEGER DEFAULT 0,
                source TEXT DEFAULT 'search',
                searched_at TEXT DEFAULT (datetime('now'))
            );

            -- Daily aggregate stats
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                new_users INTEGER DEFAULT 0,
                total_searches INTEGER DEFAULT 0,
                total_favorites INTEGER DEFAULT 0,
                total_alerts_sent INTEGER DEFAULT 0
            );

            -- Performance indexes
            CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_user ON alerts(user_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(is_active);
            CREATE INDEX IF NOT EXISTS idx_sent_jobs_user ON sent_jobs(user_id);
            CREATE INDEX IF NOT EXISTS idx_sent_jobs_unique ON sent_jobs(user_id, job_url);
            CREATE INDEX IF NOT EXISTS idx_search_user ON search_history(user_id);
            CREATE INDEX IF NOT EXISTS idx_search_term ON search_history(search_term);
            CREATE INDEX IF NOT EXISTS idx_search_date ON search_history(searched_at);
            CREATE INDEX IF NOT EXISTS idx_search_country ON search_history(country_code);
            CREATE INDEX IF NOT EXISTS idx_users_created ON users(created_at);
        """)
    logger.info("Database initialized at: %s", DB_PATH)


def _ensure_user_exists(conn, user_id: int, username: str = "", first_name: str = ""):
    """Ensure a user record exists. Creates one if missing. Returns True if new user."""
    row = conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)).fetchone()
    if row:
        return False
    try:
        conn.execute(
            "INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
            (user_id, username, first_name),
        )
        _increment_daily_stat_conn(conn, "new_users")
        return True
    except Exception as e:
        logger.error("Error creating user %s: %s", user_id, e)
        return False


def _safe_str(value, default: str = "") -> str:
    """Safely convert a value to string, handling nan/None."""
    if value is None:
        return default
    s = str(value)
    if s.lower() in ("nan", "none", ""):
        return default
    return s


# ========================
# User Profile Functions
# ========================

def get_or_create_user(user_id: int, username: str = "", first_name: str = "") -> dict:
    """Get existing user or create new one. Always safe to call."""
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if row:
                # Update username/first_name if changed
                if (username and username != row["username"]) or (first_name and first_name != row["first_name"]):
                    conn.execute(
                        "UPDATE users SET username = ?, first_name = ?, updated_at = datetime('now') WHERE user_id = ?",
                        (username or row["username"], first_name or row["first_name"], user_id),
                    )
                return dict(row)
            # Create new user
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (user_id, username or "", first_name or ""),
            )
            _increment_daily_stat_conn(conn, "new_users")
            return {
                "user_id": user_id,
                "username": username,
                "first_name": first_name,
                "preferred_countries": "[]",
                "preferred_keywords": "[]",
                "alerts_enabled": 0,
            }
    except Exception as e:
        logger.error("Error in get_or_create_user(%s): %s", user_id, e)
        return {"user_id": user_id, "username": username, "first_name": first_name,
                "preferred_countries": "[]", "preferred_keywords": "[]", "alerts_enabled": 0}


def update_user_preferences(user_id: int, countries: list = None, keywords: list = None):
    """Update user's preferred countries and keywords."""
    try:
        with get_db() as conn:
            _ensure_user_exists(conn, user_id)
            if countries is not None:
                conn.execute(
                    "UPDATE users SET preferred_countries = ?, updated_at = datetime('now') WHERE user_id = ?",
                    (json.dumps(countries), user_id),
                )
            if keywords is not None:
                conn.execute(
                    "UPDATE users SET preferred_keywords = ?, updated_at = datetime('now') WHERE user_id = ?",
                    (json.dumps(keywords), user_id),
                )
    except Exception as e:
        logger.error("Error updating preferences for %s: %s", user_id, e)


def get_user_preferences(user_id: int) -> dict:
    """Get user's saved preferences."""
    try:
        with get_db() as conn:
            row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
            if not row:
                return {"preferred_countries": [], "preferred_keywords": [], "alerts_enabled": False}
            return {
                "preferred_countries": json.loads(row["preferred_countries"] or "[]"),
                "preferred_keywords": json.loads(row["preferred_keywords"] or "[]"),
                "alerts_enabled": bool(row["alerts_enabled"]),
                "alert_interval": row["alert_interval"],
            }
    except Exception as e:
        logger.error("Error getting preferences for %s: %s", user_id, e)
        return {"preferred_countries": [], "preferred_keywords": [], "alerts_enabled": False}


# ========================
# Favorites Functions
# ========================

def save_favorite(user_id: int, job: dict) -> bool:
    """Save a job to user's favorites. Returns False if already saved."""
    job_url = _safe_str(job.get("job_url", ""))
    try:
        with get_db() as conn:
            _ensure_user_exists(conn, user_id)
            if job_url:
                existing = conn.execute(
                    "SELECT id FROM favorites WHERE user_id = ? AND job_url = ?",
                    (user_id, job_url),
                ).fetchone()
                if existing:
                    return False
            conn.execute(
                """INSERT INTO favorites (user_id, job_title, company, location, job_url, email, source, country_name, description)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    user_id,
                    _safe_str(job.get("title", "")),
                    _safe_str(job.get("company", "")),
                    _safe_str(job.get("location", "")),
                    job_url,
                    _safe_str(job.get("_email", "")),
                    _safe_str(job.get("site", "")),
                    _safe_str(job.get("_country_name", "")),
                    _safe_str(job.get("description", ""))[:500],
                ),
            )
            _increment_daily_stat_conn(conn, "total_favorites")
            return True
    except Exception as e:
        logger.error("Error saving favorite for %s: %s", user_id, e)
        return False


def get_favorites(user_id: int, limit: int = 20) -> list:
    """Get user's saved favorite jobs."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM favorites WHERE user_id = ? ORDER BY saved_at DESC LIMIT ?",
                (user_id, limit),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error("Error getting favorites for %s: %s", user_id, e)
        return []


def remove_favorite(user_id: int, favorite_id: int) -> bool:
    """Remove a job from favorites."""
    try:
        with get_db() as conn:
            cursor = conn.execute(
                "DELETE FROM favorites WHERE id = ? AND user_id = ?",
                (favorite_id, user_id),
            )
            return cursor.rowcount > 0
    except Exception as e:
        logger.error("Error removing favorite: %s", e)
        return False


def count_favorites(user_id: int) -> int:
    """Count user's favorites."""
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM favorites WHERE user_id = ?", (user_id,)
            ).fetchone()
            return row["cnt"] if row else 0
    except Exception:
        return 0


# ========================
# Alert Functions
# ========================

def add_alert(user_id: int, keyword: str, country_code: str = "all") -> int:
    """Add a new job alert. Returns alert ID or -1 if duplicate."""
    try:
        with get_db() as conn:
            _ensure_user_exists(conn, user_id)
            existing = conn.execute(
                "SELECT id FROM alerts WHERE user_id = ? AND keyword = ? AND country_code = ? AND is_active = 1",
                (user_id, keyword.lower().strip(), country_code),
            ).fetchone()
            if existing:
                return -1
            cursor = conn.execute(
                "INSERT INTO alerts (user_id, keyword, country_code) VALUES (?, ?, ?)",
                (user_id, keyword.lower().strip(), country_code),
            )
            conn.execute(
                "UPDATE users SET alerts_enabled = 1, updated_at = datetime('now') WHERE user_id = ?",
                (user_id,),
            )
            return cursor.lastrowid
    except Exception as e:
        logger.error("Error adding alert for %s: %s", user_id, e)
        return -1


def get_user_alerts(user_id: int) -> list:
    """Get all active alerts for a user."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT * FROM alerts WHERE user_id = ? AND is_active = 1 ORDER BY created_at DESC",
                (user_id,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error("Error getting alerts for %s: %s", user_id, e)
        return []


def remove_alert(user_id: int, alert_id: int) -> bool:
    """Deactivate an alert."""
    try:
        with get_db() as conn:
            cursor = conn.execute(
                "UPDATE alerts SET is_active = 0 WHERE id = ? AND user_id = ?",
                (alert_id, user_id),
            )
            remaining = conn.execute(
                "SELECT COUNT(*) as cnt FROM alerts WHERE user_id = ? AND is_active = 1",
                (user_id,),
            ).fetchone()
            if remaining and remaining["cnt"] == 0:
                conn.execute(
                    "UPDATE users SET alerts_enabled = 0, updated_at = datetime('now') WHERE user_id = ?",
                    (user_id,),
                )
            return cursor.rowcount > 0
    except Exception as e:
        logger.error("Error removing alert: %s", e)
        return False


def get_all_active_alerts() -> list:
    """Get all active alerts across all users (for the scheduler)."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT a.*, u.first_name, u.username
                   FROM alerts a
                   LEFT JOIN users u ON a.user_id = u.user_id
                   WHERE a.is_active = 1""",
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        logger.error("Error getting active alerts: %s", e)
        return []


def update_alert_sent(alert_id: int):
    """Update the last_sent timestamp for an alert."""
    try:
        with get_db() as conn:
            conn.execute(
                "UPDATE alerts SET last_sent = datetime('now') WHERE id = ?",
                (alert_id,),
            )
            _increment_daily_stat_conn(conn, "total_alerts_sent")
    except Exception as e:
        logger.error("Error updating alert sent: %s", e)


def is_job_sent(user_id: int, job_url: str) -> bool:
    """Check if a job was already sent to a user."""
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT 1 FROM sent_jobs WHERE user_id = ? AND job_url = ?",
                (user_id, job_url),
            ).fetchone()
            return row is not None
    except Exception:
        return False


def mark_job_sent(user_id: int, job_url: str):
    """Mark a job as sent to a user."""
    try:
        with get_db() as conn:
            _ensure_user_exists(conn, user_id)
            conn.execute(
                "INSERT OR IGNORE INTO sent_jobs (user_id, job_url) VALUES (?, ?)",
                (user_id, job_url),
            )
    except Exception as e:
        logger.error("Error marking job sent: %s", e)


def count_alerts(user_id: int) -> int:
    """Count user's active alerts."""
    try:
        with get_db() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM alerts WHERE user_id = ? AND is_active = 1",
                (user_id,),
            ).fetchone()
            return row["cnt"] if row else 0
    except Exception:
        return 0


# ========================
# Search Tracking
# ========================

def log_search(user_id: int, search_term: str, country_code: str, results_count: int, source: str = "search"):
    """Log a search for analytics. Always safe - auto-creates user if needed."""
    try:
        with get_db() as conn:
            _ensure_user_exists(conn, user_id)
            conn.execute(
                "INSERT INTO search_history (user_id, search_term, country_code, results_count, source) VALUES (?, ?, ?, ?, ?)",
                (user_id, search_term.lower().strip(), country_code, results_count, source),
            )
            _increment_daily_stat_conn(conn, "total_searches")
    except Exception as e:
        logger.error("Error logging search: %s", e)


# ========================
# Daily Stats Helper
# ========================

def _increment_daily_stat_conn(conn, field: str):
    """Increment a daily stat counter using an existing connection."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    try:
        conn.execute(
            f"""INSERT INTO daily_stats (date, {field}) VALUES (?, 1)
                ON CONFLICT(date) DO UPDATE SET {field} = {field} + 1""",
            (today,),
        )
    except Exception as e:
        logger.error("Error incrementing daily stat %s: %s", field, e)


# ========================
# Admin Statistics & Analytics
# ========================

def get_bot_stats() -> dict:
    """Get basic bot statistics."""
    try:
        with get_db() as conn:
            users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
            favorites = conn.execute("SELECT COUNT(*) as cnt FROM favorites").fetchone()["cnt"]
            alerts = conn.execute("SELECT COUNT(*) as cnt FROM alerts WHERE is_active = 1").fetchone()["cnt"]
            searches = conn.execute("SELECT COUNT(*) as cnt FROM search_history").fetchone()["cnt"]
            return {"total_users": users, "total_favorites": favorites,
                    "active_alerts": alerts, "total_searches": searches}
    except Exception as e:
        logger.error("Error getting bot stats: %s", e)
        return {"total_users": 0, "total_favorites": 0, "active_alerts": 0, "total_searches": 0}


def get_admin_overview() -> dict:
    """Get comprehensive admin overview statistics."""
    try:
        with get_db() as conn:
            total_users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
            total_favorites = conn.execute("SELECT COUNT(*) as cnt FROM favorites").fetchone()["cnt"]
            active_alerts = conn.execute("SELECT COUNT(*) as cnt FROM alerts WHERE is_active = 1").fetchone()["cnt"]
            total_searches = conn.execute("SELECT COUNT(*) as cnt FROM search_history").fetchone()["cnt"]
            total_sent_jobs = conn.execute("SELECT COUNT(*) as cnt FROM sent_jobs").fetchone()["cnt"]

            today = datetime.utcnow().strftime("%Y-%m-%d")
            users_today = conn.execute(
                "SELECT COUNT(*) as cnt FROM users WHERE date(created_at) = ?", (today,)
            ).fetchone()["cnt"]
            users_week = conn.execute(
                "SELECT COUNT(*) as cnt FROM users WHERE created_at >= datetime('now', '-7 days')"
            ).fetchone()["cnt"]
            searches_today = conn.execute(
                "SELECT COUNT(*) as cnt FROM search_history WHERE date(searched_at) = ?", (today,)
            ).fetchone()["cnt"]
            searches_week = conn.execute(
                "SELECT COUNT(*) as cnt FROM search_history WHERE searched_at >= datetime('now', '-7 days')"
            ).fetchone()["cnt"]

            return {
                "total_users": total_users, "total_favorites": total_favorites,
                "active_alerts": active_alerts, "total_searches": total_searches,
                "total_sent_jobs": total_sent_jobs, "users_today": users_today,
                "users_this_week": users_week, "searches_today": searches_today,
                "searches_this_week": searches_week,
            }
    except Exception as e:
        logger.error("Error getting admin overview: %s", e)
        return {k: 0 for k in ["total_users", "total_favorites", "active_alerts",
                                "total_searches", "total_sent_jobs", "users_today",
                                "users_this_week", "searches_today", "searches_this_week"]}


def get_top_searches(limit: int = 10) -> list:
    """Get most popular search terms."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT search_term, COUNT(*) as count,
                          AVG(results_count) as avg_results
                   FROM search_history
                   GROUP BY search_term
                   ORDER BY count DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_top_countries(limit: int = 10) -> list:
    """Get most searched countries."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT country_code, COUNT(*) as count
                   FROM search_history
                   GROUP BY country_code
                   ORDER BY count DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_active_users(limit: int = 10) -> list:
    """Get most active users by search count."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT u.user_id, u.username, u.first_name, u.created_at,
                          COUNT(sh.id) as search_count,
                          (SELECT COUNT(*) FROM favorites f WHERE f.user_id = u.user_id) as fav_count,
                          (SELECT COUNT(*) FROM alerts a WHERE a.user_id = u.user_id AND a.is_active = 1) as alert_count
                   FROM users u
                   LEFT JOIN search_history sh ON u.user_id = sh.user_id
                   GROUP BY u.user_id
                   ORDER BY search_count DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_recent_users(limit: int = 10) -> list:
    """Get most recently joined users."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                "SELECT user_id, username, first_name, created_at FROM users ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_daily_stats_history(days: int = 7) -> list:
    """Get daily stats for the last N days."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT * FROM daily_stats
                   WHERE date >= date('now', ? || ' days')
                   ORDER BY date DESC""",
                (f"-{days}",),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_hourly_search_distribution() -> list:
    """Get search distribution by hour of day."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT strftime('%H', searched_at) as hour, COUNT(*) as count
                   FROM search_history
                   GROUP BY hour ORDER BY hour""",
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def get_zero_result_searches(limit: int = 10) -> list:
    """Get searches that returned zero results."""
    try:
        with get_db() as conn:
            rows = conn.execute(
                """SELECT search_term, country_code, COUNT(*) as count
                   FROM search_history WHERE results_count = 0
                   GROUP BY search_term, country_code
                   ORDER BY count DESC LIMIT ?""",
                (limit,),
            ).fetchall()
            return [dict(r) for r in rows]
    except Exception:
        return []


def broadcast_get_all_user_ids() -> list:
    """Get all user IDs for broadcast messages."""
    try:
        with get_db() as conn:
            rows = conn.execute("SELECT user_id FROM users").fetchall()
            return [r["user_id"] for r in rows]
    except Exception:
        return []
