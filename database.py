"""
LinkedIt Bot - Database Module
SQLite database for user profiles, favorites, and job alerts.
Designed to work on Render with persistent disk or ephemeral storage.
"""

import sqlite3
import json
import logging
import os
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Database file path - can be overridden via env var for persistent storage on Render
DB_PATH = os.environ.get("DB_PATH", "linkedit.db")


@contextmanager
def get_db():
    """Thread-safe database connection context manager."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize database tables."""
    with get_db() as conn:
        conn.executescript("""
            -- User profiles table
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                preferred_countries TEXT DEFAULT '[]',
                preferred_keywords TEXT DEFAULT '[]',
                alerts_enabled INTEGER DEFAULT 0,
                alert_interval TEXT DEFAULT 'daily',
                language TEXT DEFAULT 'ar',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            -- Saved/favorite jobs table
            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                job_title TEXT,
                company TEXT,
                location TEXT,
                job_url TEXT,
                email TEXT,
                source TEXT,
                country_name TEXT,
                description TEXT,
                saved_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            -- Job alerts table
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                country_code TEXT DEFAULT 'all',
                is_active INTEGER DEFAULT 1,
                last_sent TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            -- Sent jobs tracking (to avoid duplicates in alerts)
            CREATE TABLE IF NOT EXISTS sent_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                job_url TEXT NOT NULL,
                sent_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                UNIQUE(user_id, job_url)
            );

            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_user ON alerts(user_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(is_active);
            CREATE INDEX IF NOT EXISTS idx_sent_jobs_user ON sent_jobs(user_id);
        """)
    logger.info("Database initialized at: %s", DB_PATH)


# ========================
# User Profile Functions
# ========================

def get_or_create_user(user_id: int, username: str = "", first_name: str = "") -> dict:
    """Get existing user or create new one."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if row:
            return dict(row)
        conn.execute(
            "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
            (user_id, username or "", first_name or ""),
        )
        return {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "preferred_countries": "[]",
            "preferred_keywords": "[]",
            "alerts_enabled": 0,
            "alert_interval": "daily",
        }


def update_user_preferences(user_id: int, countries: list = None, keywords: list = None):
    """Update user's preferred countries and keywords."""
    with get_db() as conn:
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


def get_user_preferences(user_id: int) -> dict:
    """Get user's saved preferences."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if not row:
            return {"preferred_countries": [], "preferred_keywords": []}
        return {
            "preferred_countries": json.loads(row["preferred_countries"] or "[]"),
            "preferred_keywords": json.loads(row["preferred_keywords"] or "[]"),
            "alerts_enabled": bool(row["alerts_enabled"]),
            "alert_interval": row["alert_interval"],
        }


# ========================
# Favorites Functions
# ========================

def save_favorite(user_id: int, job: dict) -> bool:
    """Save a job to user's favorites. Returns False if already saved."""
    job_url = str(job.get("job_url", ""))
    with get_db() as conn:
        # Check if already saved
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
                str(job.get("title", "")),
                str(job.get("company", "")),
                str(job.get("location", "")),
                job_url,
                str(job.get("_email", "")),
                str(job.get("site", "")),
                str(job.get("_country_name", "")),
                str(job.get("description", ""))[:500],
            ),
        )
        return True


def get_favorites(user_id: int, limit: int = 20) -> list:
    """Get user's saved favorite jobs."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM favorites WHERE user_id = ? ORDER BY saved_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def remove_favorite(user_id: int, favorite_id: int) -> bool:
    """Remove a job from favorites."""
    with get_db() as conn:
        cursor = conn.execute(
            "DELETE FROM favorites WHERE id = ? AND user_id = ?",
            (favorite_id, user_id),
        )
        return cursor.rowcount > 0


def count_favorites(user_id: int) -> int:
    """Count user's favorites."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM favorites WHERE user_id = ?", (user_id,)
        ).fetchone()
        return row["cnt"] if row else 0


# ========================
# Alert Functions
# ========================

def add_alert(user_id: int, keyword: str, country_code: str = "all") -> int:
    """Add a new job alert. Returns alert ID."""
    with get_db() as conn:
        # Check for duplicate
        existing = conn.execute(
            "SELECT id FROM alerts WHERE user_id = ? AND keyword = ? AND country_code = ? AND is_active = 1",
            (user_id, keyword.lower().strip(), country_code),
        ).fetchone()
        if existing:
            return -1  # Already exists

        cursor = conn.execute(
            "INSERT INTO alerts (user_id, keyword, country_code) VALUES (?, ?, ?)",
            (user_id, keyword.lower().strip(), country_code),
        )
        # Enable alerts for user
        conn.execute(
            "UPDATE users SET alerts_enabled = 1, updated_at = datetime('now') WHERE user_id = ?",
            (user_id,),
        )
        return cursor.lastrowid


def get_user_alerts(user_id: int) -> list:
    """Get all active alerts for a user."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts WHERE user_id = ? AND is_active = 1 ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def remove_alert(user_id: int, alert_id: int) -> bool:
    """Deactivate an alert."""
    with get_db() as conn:
        cursor = conn.execute(
            "UPDATE alerts SET is_active = 0 WHERE id = ? AND user_id = ?",
            (alert_id, user_id),
        )
        # Check if user has any remaining active alerts
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


def get_all_active_alerts() -> list:
    """Get all active alerts across all users (for the alert scheduler)."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT a.*, u.first_name, u.username
               FROM alerts a
               JOIN users u ON a.user_id = u.user_id
               WHERE a.is_active = 1""",
        ).fetchall()
        return [dict(r) for r in rows]


def update_alert_sent(alert_id: int):
    """Update the last_sent timestamp for an alert."""
    with get_db() as conn:
        conn.execute(
            "UPDATE alerts SET last_sent = datetime('now') WHERE id = ?",
            (alert_id,),
        )


def is_job_sent(user_id: int, job_url: str) -> bool:
    """Check if a job was already sent to a user."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM sent_jobs WHERE user_id = ? AND job_url = ?",
            (user_id, job_url),
        ).fetchone()
        return row is not None


def mark_job_sent(user_id: int, job_url: str):
    """Mark a job as sent to a user."""
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO sent_jobs (user_id, job_url) VALUES (?, ?)",
                (user_id, job_url),
            )
        except sqlite3.IntegrityError:
            pass


def count_alerts(user_id: int) -> int:
    """Count user's active alerts."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM alerts WHERE user_id = ? AND is_active = 1",
            (user_id,),
        ).fetchone()
        return row["cnt"] if row else 0


# ========================
# Statistics
# ========================

def get_bot_stats() -> dict:
    """Get basic bot statistics."""
    with get_db() as conn:
        users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
        favorites = conn.execute("SELECT COUNT(*) as cnt FROM favorites").fetchone()["cnt"]
        alerts = conn.execute("SELECT COUNT(*) as cnt FROM alerts WHERE is_active = 1").fetchone()["cnt"]
        return {
            "total_users": users,
            "total_favorites": favorites,
            "active_alerts": alerts,
        }
