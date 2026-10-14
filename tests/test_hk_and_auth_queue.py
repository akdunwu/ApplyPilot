"""Unit tests for Hong Kong portals adaptation and async auth-queue parallel progression."""

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from applypilot.database import (
    init_db,
    get_connection,
    close_connection,
    _ALL_COLUMNS,
    get_pending_auth_jobs,
    resume_auth_job,
    resume_all_auth_jobs,
)
from applypilot.apply.launcher import acquire_job


class TestHKAndAuthQueue(unittest.TestCase):
    def setUp(self):
        """Create a temporary SQLite database for testing."""
        self._temp_file = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._temp_file.close()
        self.db_path = Path(self._temp_file.name)

        # Initialize schema
        conn = init_db(self.db_path)
        conn.close()

        self.patcher1 = patch("applypilot.config.DB_PATH", self.db_path)
        self.patcher2 = patch("applypilot.database.DB_PATH", self.db_path)
        self.patcher1.start()
        self.patcher2.start()

    def tearDown(self):
        """Clean up database and patches."""
        self.patcher1.stop()
        self.patcher2.stop()
        close_connection(self.db_path)
        if self.db_path.exists():
            try:
                self.db_path.unlink()
            except Exception:
                pass

    def test_schema_has_auth_columns(self):
        """Verify that auth-related columns exist in _ALL_COLUMNS and SQLite schema."""
        self.assertIn("portal_url", _ALL_COLUMNS)
        self.assertIn("auth_prompted_at", _ALL_COLUMNS)
        self.assertIn("auth_note", _ALL_COLUMNS)

        conn = get_connection(self.db_path)
        cursor = conn.execute("PRAGMA table_info(jobs)")
        cols = {row["name"] for row in cursor.fetchall()}
        self.assertIn("portal_url", cols)
        self.assertIn("auth_prompted_at", cols)
        self.assertIn("auth_note", cols)

    def test_auth_queue_and_resume_flow(self):
        """Verify that jobs placed in needs_auth can be listed, filtered, and resumed."""
        conn = get_connection(self.db_path)

        # Insert test jobs
        conn.execute("""
            INSERT INTO jobs (url, title, site, apply_status, portal_url, auth_prompted_at, auth_note, fit_score)
            VALUES 
                ('https://jobs.example.com/job1', 'AI Architect', 'Morgan Stanley', 'needs_auth', 'https://ms.wd3.myworkdayjobs.com/login', '2026-10-14T10:00:00Z', 'Workday login required', 9),
                ('https://jobs.example.com/job2', 'FinTech Engineer', 'HSBC', 'applied', NULL, NULL, NULL, 8),
                ('https://jobs.example.com/job3', 'Cloud Solutions', 'Standard Chartered', 'needs_auth', 'https://sc.taleo.net/login', '2026-10-14T10:05:00Z', 'Taleo account required', 7)
        """)
        conn.commit()

        # 1. Query pending auth jobs
        pending = get_pending_auth_jobs(conn)
        self.assertEqual(len(pending), 2)

        # 2. Resume a single job
        resumed = resume_auth_job("https://jobs.example.com/job1", conn)
        self.assertTrue(resumed)

        # Remaining in auth queue should be 1
        pending_after_one = get_pending_auth_jobs(conn)
        self.assertEqual(len(pending_after_one), 1)
        self.assertEqual(pending_after_one[0]["url"], "https://jobs.example.com/job3")

        # Job 1 apply_status should now be NULL (ready for re-apply)
        row = conn.execute("SELECT apply_status FROM jobs WHERE url = 'https://jobs.example.com/job1'").fetchone()
        self.assertIsNone(row["apply_status"])

        # 3. Resume all remaining jobs
        count = resume_all_auth_jobs(conn)
        self.assertEqual(count, 1)
        self.assertEqual(len(get_pending_auth_jobs(conn)), 0)

    def test_worker_queue_parallel_progression_when_needs_auth(self):
        """Verify that jobs in needs_auth are skipped so workers keep progressing in parallel."""
        conn = get_connection(self.db_path)

        # Insert Job 1 (needs_auth) and Job 2 (ready to apply)
        conn.execute("""
            INSERT INTO jobs (url, title, site, tailored_resume_path, fit_score, apply_status)
            VALUES 
                ('https://jobs.example.com/job1', 'Suspended Job', 'Company A', '/path/resume1.pdf', 9, 'needs_auth'),
                ('https://jobs.example.com/job2', 'Next Parallel Job', 'Company B', '/path/resume2.pdf', 8, NULL)
        """)
        conn.commit()

        # acquire_job should immediately skip Job 1 and acquire Job 2
        acquired = acquire_job(min_score=7, worker_id=0)
        self.assertIsNotNone(acquired)
        self.assertEqual(acquired["url"], "https://jobs.example.com/job2")
        self.assertEqual(acquired["title"], "Next Parallel Job")

    def test_hk_portals_module_import(self):
        """Verify hk_portals functions can be imported and are callable."""
        from applypilot.discovery.hk_portals import (
            scrape_jobsdb,
            scrape_efinancialcareers,
            scrape_offertoday,
            discover_hk_jobs,
        )
        self.assertTrue(callable(scrape_jobsdb))
        self.assertTrue(callable(scrape_efinancialcareers))
        self.assertTrue(callable(scrape_offertoday))
        self.assertTrue(callable(discover_hk_jobs))


if __name__ == "__main__":
    unittest.main()
