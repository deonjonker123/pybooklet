import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any

DATABASE_NAME = "pybooklet.db"


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_db_connection():
    """Get a database connection with row factory for dict-like access."""
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn


# ============================================================================
# BOOKS - CORE OPERATIONS
# ============================================================================

def get_all_books(
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "date_added",
        search: str = None
) -> tuple[List[Dict], int]:
    """
    Get all books from library (not in any other status).
    Returns (books, total_count).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Build the WHERE clause for search
    where_conditions = []
    params = []

    # Exclude books that are in other states
    where_conditions.append("""
        id NOT IN (SELECT book_id FROM reading_tracker)
        AND id NOT IN (SELECT book_id FROM completed_books)
        AND id NOT IN (SELECT book_id FROM abandoned_books)
    """)

    if search:
        where_conditions.append("""
            (title LIKE ? OR author LIKE ? OR series LIKE ? OR genre LIKE ?)
        """)
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param, search_param])

    where_clause = " AND ".join(where_conditions)

    # Get total count
    count_query = f"SELECT COUNT(*) as count FROM books WHERE {where_clause}"
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()["count"]

    # Get books with sorting and pagination
    valid_sorts = {
        "author": "author",
        "title": "title",
        "page_count": "page_count",
        "date_added": "date_added"
    }
    sort_column = valid_sorts.get(sort_by, "date_added")

    query = f"""
        SELECT * FROM books 
        WHERE {where_clause}
        ORDER BY {sort_column} DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    cursor.execute(query, params)
    books = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return books, total_count


def get_book_by_id(book_id: int) -> Optional[Dict]:
    """Get a single book by ID."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM books WHERE id = ?", (book_id,))
    row = cursor.fetchone()

    conn.close()
    return dict(row) if row else None


def add_book(
        title: str,
        author: str,
        page_count: int,
        cover_url: str = None,
        series: str = None,
        series_number: float = None,
        synopsis: str = None,
        genre: str = None
) -> int:
    """Add a new book to the library. Returns the book ID."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   INSERT INTO books (cover_url, title, series, series_number, author, page_count, synopsis, genre)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   """, (cover_url, title, series, series_number, author, page_count, synopsis, genre))

    book_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return book_id


def update_book(
        book_id: int,
        title: str = None,
        author: str = None,
        page_count: int = None,
        cover_url: str = None,
        series: str = None,
        series_number: float = None,
        synopsis: str = None,
        genre: str = None
) -> bool:
    """Update book details. Only updates provided fields."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Build dynamic update query
    updates = []
    params = []

    if title is not None:
        updates.append("title = ?")
        params.append(title)
    if author is not None:
        updates.append("author = ?")
        params.append(author)
    if page_count is not None:
        updates.append("page_count = ?")
        params.append(page_count)
    if cover_url is not None:
        updates.append("cover_url = ?")
        params.append(cover_url)
    if series is not None:
        updates.append("series = ?")
        params.append(series)
    if series_number is not None:
        updates.append("series_number = ?")
        params.append(series_number)
    if synopsis is not None:
        updates.append("synopsis = ?")
        params.append(synopsis)
    if genre is not None:
        updates.append("genre = ?")
        params.append(genre)

    if not updates:
        conn.close()
        return False

    params.append(book_id)
    query = f"UPDATE books SET {', '.join(updates)} WHERE id = ?"

    cursor.execute(query, params)
    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


def delete_book(book_id: int) -> bool:
    """
    Permanently delete a book from the database.
    Also removes all related data (tracker, completed, abandoned, sessions).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Delete from all tables
    cursor.execute("DELETE FROM reading_sessions WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM books WHERE id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


# ============================================================================
# BOOK STATUS OPERATIONS
# ============================================================================

def get_book_status(book_id: int) -> str:
    """
    Get the current status of a book.
    Returns: 'library', 'tracking', 'completed', 'abandoned', or 'unknown'
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Check if book exists
    cursor.execute("SELECT id FROM books WHERE id = ?", (book_id,))
    if not cursor.fetchone():
        conn.close()
        return "unknown"

    # Check tracking
    cursor.execute("SELECT id FROM reading_tracker WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "tracking"

    # Check completed
    cursor.execute("SELECT id FROM completed_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "completed"

    # Check abandoned
    cursor.execute("SELECT id FROM abandoned_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "abandoned"

    conn.close()
    return "library"


# ============================================================================
# READING TRACKER OPERATIONS
# ============================================================================

def get_tracked_books() -> List[Dict]:
    """Get all currently tracked books with their progress."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT b.*,
                          rt.id as tracker_id,
                          rt.current_page,
                          rt.start_date
                   FROM books b
                            JOIN reading_tracker rt ON b.id = rt.book_id
                   ORDER BY rt.start_date DESC
                   """)

    books = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return books


def add_to_tracker(book_id: int, current_page: int = 0) -> bool:
    """
    Move a book from library to reading tracker.
    Ensures book is removed from other statuses first.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Remove from other statuses
    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))

    # Check if already in tracker
    cursor.execute("SELECT id FROM reading_tracker WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return False  # Already tracking

    # Add to tracker
    cursor.execute("""
                   INSERT INTO reading_tracker (book_id, current_page)
                   VALUES (?, ?)
                   """, (book_id, current_page))

    conn.commit()
    conn.close()

    return True


def update_tracker_progress(book_id: int, current_page: int) -> bool:
    """Update the current page for a tracked book."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   UPDATE reading_tracker
                   SET current_page = ?
                   WHERE book_id = ?
                   """, (current_page, book_id))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


def remove_from_tracker(book_id: int) -> bool:
    """Remove a book from tracker and return it to library."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


# ============================================================================
# COMPLETED BOOKS OPERATIONS
# ============================================================================

def get_completed_books(
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "completion_date",
        search: str = None,
        year: int = None,
        rating: int = None
) -> tuple[List[Dict], int]:
    """
    Get all completed books with optional filters.
    Returns (books, total_count).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    where_conditions = []
    params = []

    if search:
        where_conditions.append("""
            (b.title LIKE ? OR b.author LIKE ? OR b.series LIKE ? OR b.genre LIKE ?)
        """)
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param, search_param])

    if year:
        where_conditions.append("strftime('%Y', cb.completion_date) = ?")
        params.append(str(year))

    if rating:
        where_conditions.append("cb.rating = ?")
        params.append(rating)

    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

    # Get total count
    count_query = f"""
        SELECT COUNT(*) as count 
        FROM books b
        JOIN completed_books cb ON b.id = cb.book_id
        WHERE {where_clause}
    """
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()["count"]

    # Get books
    valid_sorts = {
        "author": "b.author",
        "title": "b.title",
        "page_count": "b.page_count",
        "completion_date": "cb.completion_date"
    }
    sort_column = valid_sorts.get(sort_by, "cb.completion_date")

    query = f"""
        SELECT 
        b.*,
        cb.id as completion_id,
        cb.rating,
        cb.review,
        cb.start_date,
        cb.completion_date,
        CAST(JULIANDAY(cb.completion_date) - JULIANDAY(cb.start_date) AS INTEGER) as duration_days
        FROM books b
        JOIN completed_books cb ON b.id = cb.book_id
        WHERE {where_clause}
        ORDER BY {sort_column} DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    cursor.execute(query, params)
    books = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return books, total_count


def complete_book(
        book_id: int,
        rating: int = None,
        review: str = None,
        start_date: str = None
) -> bool:
    """
    Move a book from tracker to completed.
    Removes from tracker and other statuses.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get start date from tracker if not provided
    if not start_date:
        cursor.execute("SELECT start_date FROM reading_tracker WHERE book_id = ?", (book_id,))
        row = cursor.fetchone()
        if row:
            start_date = row["start_date"]

    # Remove from other statuses
    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))

    # Add to completed
    cursor.execute("""
                   INSERT INTO completed_books (book_id, rating, review, start_date)
                   VALUES (?, ?, ?, ?)
                   """, (book_id, rating, review, start_date))

    conn.commit()
    conn.close()

    return True


def update_completed_book(
        book_id: int,
        rating: int = None,
        review: str = None,
        start_date: str = None,
        completion_date: str = None
) -> bool:
    """Update completed book details."""
    conn = get_db_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    # ALWAYS update these fields if they're in the form submission
    updates.append("rating = ?")
    params.append(rating)

    updates.append("review = ?")
    params.append(review)

    updates.append("start_date = ?")
    params.append(start_date)

    updates.append("completion_date = ?")
    params.append(completion_date)

    params.append(book_id)
    query = f"UPDATE completed_books SET {', '.join(updates)} WHERE book_id = ?"

    cursor.execute(query, params)
    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


def remove_from_completed(book_id: int) -> bool:
    """Remove a book from completed and return it to library."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


# ============================================================================
# ABANDONED BOOKS OPERATIONS
# ============================================================================

def get_abandoned_books(
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "abandonment_date",
        search: str = None
) -> tuple[List[Dict], int]:
    """
    Get all abandoned books.
    Returns (books, total_count).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    where_conditions = []
    params = []

    if search:
        where_conditions.append("""
            (b.title LIKE ? OR b.author LIKE ? OR b.series LIKE ? OR b.genre LIKE ?)
        """)
        search_param = f"%{search}%"
        params.extend([search_param, search_param, search_param, search_param])

    where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"

    # Get total count
    count_query = f"""
        SELECT COUNT(*) as count 
        FROM books b
        JOIN abandoned_books ab ON b.id = ab.book_id
        WHERE {where_clause}
    """
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()["count"]

    # Get books
    valid_sorts = {
        "author": "b.author",
        "title": "b.title",
        "page_count": "b.page_count",
        "abandonment_date": "ab.abandonment_date"
    }
    sort_column = valid_sorts.get(sort_by, "ab.abandonment_date")

    query = f"""
        SELECT 
            b.*,
            ab.id as abandonment_id,
            ab.page_at_abandonment,
            ab.reason,
            ab.start_date,
            ab.abandonment_date
        FROM books b
        JOIN abandoned_books ab ON b.id = ab.book_id
        WHERE {where_clause}
        ORDER BY {sort_column} DESC
        LIMIT ? OFFSET ?
    """
    params.extend([limit, offset])

    cursor.execute(query, params)
    books = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return books, total_count


def abandon_book(
        book_id: int,
        page_at_abandonment: int,
        reason: str = None,
        start_date: str = None
) -> bool:
    """
    Move a book from tracker to abandoned.
    Removes from tracker and other statuses.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get start date from tracker if not provided
    if not start_date:
        cursor.execute("SELECT start_date FROM reading_tracker WHERE book_id = ?", (book_id,))
        row = cursor.fetchone()
        if row:
            start_date = row["start_date"]

    # Remove from other statuses
    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))

    # Add to abandoned
    cursor.execute("""
                   INSERT INTO abandoned_books (book_id, page_at_abandonment, reason, start_date)
                   VALUES (?, ?, ?, ?)
                   """, (book_id, page_at_abandonment, reason, start_date))

    conn.commit()
    conn.close()

    return True


def update_abandoned_book(
        book_id: int,
        page_at_abandonment: int = None,
        reason: str = None,
        start_date: str = None,
        abandonment_date: str = None
) -> bool:
    """Update abandoned book details."""
    conn = get_db_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    # ALWAYS update these fields
    updates.append("page_at_abandonment = ?")
    params.append(page_at_abandonment)

    updates.append("reason = ?")
    params.append(reason)

    updates.append("start_date = ?")
    params.append(start_date)

    updates.append("abandonment_date = ?")
    params.append(abandonment_date)

    params.append(book_id)
    query = f"UPDATE abandoned_books SET {', '.join(updates)} WHERE book_id = ?"

    cursor.execute(query, params)
    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


def remove_from_abandoned(book_id: int) -> bool:
    """Remove a book from abandoned and return it to library."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


# ============================================================================
# READING SESSIONS OPERATIONS
# ============================================================================

def add_reading_session(
        book_id: int,
        start_time: str,
        end_time: str,
        duration_seconds: int,
        start_page: int,
        end_page: int
) -> int:
    """
    Add a new reading session.
    Returns the session ID.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    pages_read = end_page - start_page
    session_date = datetime.fromisoformat(start_time).date().isoformat()

    cursor.execute("""
                   INSERT INTO reading_sessions
                   (book_id, start_time, end_time, duration_seconds, pages_read, start_page, end_page, session_date)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                   """,
                   (book_id, start_time, end_time, duration_seconds, pages_read, start_page, end_page, session_date))

    session_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return session_id


def get_book_sessions(
        book_id: int,
        limit: int = 50,
        offset: int = 0
) -> tuple[List[Dict], int]:
    """
    Get all reading sessions for a specific book.
    Returns (sessions, total_count).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get total count
    cursor.execute("""
                   SELECT COUNT(*) as count
                   FROM reading_sessions
                   WHERE book_id = ?
                   """, (book_id,))
    total_count = cursor.fetchone()["count"]

    # Get sessions
    cursor.execute("""
                   SELECT *
                   FROM reading_sessions
                   WHERE book_id = ?
                   ORDER BY session_date DESC, start_time DESC LIMIT ?
                   OFFSET ?
                   """, (book_id, limit, offset))

    sessions = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return sessions, total_count


def get_weekly_sessions(book_id: int, week_start: str) -> List[Dict]:
    """
    Get all sessions for a book in a specific week (Sunday to Saturday).
    week_start should be the Sunday date in ISO format (YYYY-MM-DD).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Calculate week end (Saturday)
    from datetime import datetime, timedelta
    start_date = datetime.fromisoformat(week_start)
    end_date = start_date + timedelta(days=6)

    cursor.execute("""
                   SELECT *
                   FROM reading_sessions
                   WHERE book_id = ?
                     AND session_date BETWEEN ? AND ?
                   ORDER BY session_date, start_time
                   """, (book_id, week_start, end_date.date().isoformat()))

    sessions = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return sessions

def get_all_sessions(
        limit: int = 50,
        offset: int = 0
) -> tuple[List[Dict], int]:
    """
    Get all reading sessions across ALL books.
    Returns (sessions, total_count).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Get total count
    cursor.execute("""
                   SELECT COUNT(*) as count
                   FROM reading_sessions
                   """)
    total_count = cursor.fetchone()["count"]

    # Get sessions with book info
    cursor.execute("""
                   SELECT rs.*, b.title, b.author, b.cover_url
                   FROM reading_sessions rs
                   JOIN books b ON rs.book_id = b.id
                   ORDER BY rs.session_date DESC, rs.start_time DESC 
                   LIMIT ? OFFSET ?
                   """, (limit, offset))

    sessions = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return sessions, total_count


def get_all_weekly_sessions(week_start: str) -> List[Dict]:
    """
    Get all sessions across ALL books in a specific week (Sunday to Saturday).
    week_start should be the Sunday date in ISO format (YYYY-MM-DD).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    # Calculate week end (Saturday)
    from datetime import datetime, timedelta
    start_date = datetime.fromisoformat(week_start)
    end_date = start_date + timedelta(days=6)

    cursor.execute("""
                   SELECT rs.*, b.title, b.author, b.cover_url
                   FROM reading_sessions rs
                   JOIN books b ON rs.book_id = b.id
                   WHERE rs.session_date BETWEEN ? AND ?
                   ORDER BY rs.session_date, rs.start_time
                   """, (week_start, end_date.date().isoformat()))

    sessions = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return sessions


def delete_session(session_id: int) -> bool:
    """Delete a reading session."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM reading_sessions WHERE id = ?", (session_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


# ============================================================================
# STATISTICS QUERIES
# ============================================================================

def get_dashboard_stats() -> Dict:
    """Get quick stats for the dashboard."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Total books in library (unread)
    cursor.execute("""
                   SELECT COUNT(*) as count
                   FROM books
                   WHERE id NOT IN (SELECT book_id FROM reading_tracker)
                     AND id NOT IN (SELECT book_id FROM completed_books)
                     AND id NOT IN (SELECT book_id FROM abandoned_books)
                   """)
    library_count = cursor.fetchone()["count"]

    # Total books (all)
    cursor.execute("SELECT COUNT(*) as count FROM books")
    total_books = cursor.fetchone()["count"]

    # Completed books
    cursor.execute("SELECT COUNT(*) as count FROM completed_books")
    completed_count = cursor.fetchone()["count"]

    # Abandoned books
    cursor.execute("SELECT COUNT(*) as count FROM abandoned_books")
    abandoned_count = cursor.fetchone()["count"]

    # Currently tracking
    cursor.execute("SELECT COUNT(*) as count FROM reading_tracker")
    tracking_count = cursor.fetchone()["count"]

    conn.close()

    return {
        "total_books": total_books,
        "library_count": library_count,
        "completed_count": completed_count,
        "abandoned_count": abandoned_count,
        "tracking_count": tracking_count
    }


def get_year_stats(year: int = None) -> Dict:
    """Get statistics for a specific year or all time."""
    conn = get_db_connection()
    cursor = conn.cursor()

    # Completed books count
    if year:
        cursor.execute("""
                       SELECT COUNT(*) as count
                       FROM completed_books
                       WHERE strftime('%Y', completion_date) = ?
                       """, (str(year),))
    else:
        cursor.execute("SELECT COUNT(*) as count FROM completed_books")

    completed_count = cursor.fetchone()["count"]

    # Pages read (from completed books only)
    if year:
        cursor.execute("""
                       SELECT COALESCE(SUM(b.page_count), 0) as total_pages
                       FROM completed_books cb
                                JOIN books b ON cb.book_id = b.id
                       WHERE strftime('%Y', cb.completion_date) = ?
                       """, (str(year),))
    else:
        cursor.execute("""
                       SELECT COALESCE(SUM(b.page_count), 0) as total_pages
                       FROM completed_books cb
                                JOIN books b ON cb.book_id = b.id
                       """)

    pages_read = cursor.fetchone()["total_pages"]

    # Abandoned books count
    if year:
        cursor.execute("""
                       SELECT COUNT(*) as count
                       FROM abandoned_books
                       WHERE strftime('%Y', abandonment_date) = ?
                       """, (str(year),))
    else:
        cursor.execute("SELECT COUNT(*) as count FROM abandoned_books")

    abandoned_count = cursor.fetchone()["count"]

    # Average time to finish (in days)
    if year:
        cursor.execute("""
                       SELECT AVG(JULIANDAY(completion_date) - JULIANDAY(start_date)) as avg_days
                       FROM completed_books
                       WHERE start_date IS NOT NULL
                         AND strftime('%Y', completion_date) = ?
                       """, (str(year),))
    else:
        cursor.execute("""
                       SELECT AVG(JULIANDAY(completion_date) - JULIANDAY(start_date)) as avg_days
                       FROM completed_books
                       WHERE start_date IS NOT NULL
                       """)

    avg_days_result = cursor.fetchone()["avg_days"]
    avg_days = round(avg_days_result, 1) if avg_days_result else 0

    conn.close()

    return {
        "completed_count": completed_count,
        "pages_read": pages_read,
        "abandoned_count": abandoned_count,
        "avg_days_to_finish": avg_days
    }


def get_monthly_reading_data(year: int) -> List[Dict]:
    """Get books and pages read per month for a given year."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT strftime('%m', completion_date) as month,
            COUNT(*) as books_completed,
            SUM(b.page_count) as pages_read
                   FROM completed_books cb
                       JOIN books b
                   ON cb.book_id = b.id
                   WHERE strftime('%Y', completion_date) = ?
                   GROUP BY month
                   ORDER BY month
                   """, (str(year),))

    data = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return data


def get_top_authors(year: int = None, limit: int = 10) -> List[Dict]:
    """Get top authors by number of books completed."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if year:
        cursor.execute("""
                       SELECT b.author,
                              COUNT(*) as book_count
                       FROM completed_books cb
                                JOIN books b ON cb.book_id = b.id
                       WHERE strftime('%Y', cb.completion_date) = ?
                       GROUP BY b.author
                       ORDER BY book_count DESC LIMIT ?
                       """, (str(year), limit))
    else:
        cursor.execute("""
                       SELECT b.author,
                              COUNT(*) as book_count
                       FROM completed_books cb
                                JOIN books b ON cb.book_id = b.id
                       GROUP BY b.author
                       ORDER BY book_count DESC LIMIT ?
                       """, (limit,))

    authors = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return authors


def get_top_genres(year: int = None, limit: int = 10) -> List[Dict]:
    """Get top genres by number of books completed."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if year:
        cursor.execute("""
                       SELECT b.genre,
                              COUNT(*) as book_count
                       FROM completed_books cb
                                JOIN books b ON cb.book_id = b.id
                       WHERE strftime('%Y', cb.completion_date) = ?
                         AND b.genre IS NOT NULL
                       GROUP BY b.genre
                       ORDER BY book_count DESC LIMIT ?
                       """, (str(year), limit))
    else:
        cursor.execute("""
                       SELECT b.genre,
                              COUNT(*) as book_count
                       FROM completed_books cb
                                JOIN books b ON cb.book_id = b.id
                       WHERE b.genre IS NOT NULL
                       GROUP BY b.genre
                       ORDER BY book_count DESC LIMIT ?
                       """, (limit,))

    genres = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return genres


def get_rating_distribution(year: int = None) -> List[Dict]:
    """Get distribution of ratings (1-5 stars)."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if year:
        cursor.execute("""
                       SELECT rating,
                              COUNT(*) as count
                       FROM completed_books
                       WHERE strftime('%Y'
                           , completion_date) = ?
                         AND rating IS NOT NULL
                       GROUP BY rating
                       ORDER BY rating
                       """, (str(year),))
    else:
        cursor.execute("""
                       SELECT rating,
                              COUNT(*) as count
                       FROM completed_books
                       WHERE rating IS NOT NULL
                       GROUP BY rating
                       ORDER BY rating
                       """)

    ratings = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return ratings


# ============================================================================
# HELPER FUNCTIONS (SERIES, AUTHOR, GENRE)
# ============================================================================

def get_books_by_series(series_name: str) -> List[Dict]:
    """Get all books in a series (across all statuses)."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT b.*,
                          CASE
                              WHEN EXISTS (SELECT 1 FROM reading_tracker WHERE book_id = b.id) THEN 'tracking'
                              WHEN EXISTS (SELECT 1 FROM completed_books WHERE book_id = b.id) THEN 'completed'
                              WHEN EXISTS (SELECT 1 FROM abandoned_books WHERE book_id = b.id) THEN 'abandoned'
                              ELSE 'library'
                              END as status
                   FROM books b
                   WHERE LOWER(b.series) = LOWER(?)
                   ORDER BY b.series_number
                   """, (series_name,))

    books = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return books


def get_books_by_author(author_name: str) -> List[Dict]:
    """Get all books by an author (across all statuses)."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT b.*,
                          CASE
                              WHEN EXISTS (SELECT 1 FROM reading_tracker WHERE book_id = b.id) THEN 'tracking'
                              WHEN EXISTS (SELECT 1 FROM completed_books WHERE book_id = b.id) THEN 'completed'
                              WHEN EXISTS (SELECT 1 FROM abandoned_books WHERE book_id = b.id) THEN 'abandoned'
                              ELSE 'library'
                              END as status
                   FROM books b
                   WHERE LOWER(b.author) = LOWER(?)
                   ORDER BY b.date_added DESC
                   """, (author_name,))

    books = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return books


def get_books_by_genre(genre_name: str) -> List[Dict]:
    """Get all books with a specific genre (across all statuses)."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT b.*,
                          CASE
                              WHEN EXISTS (SELECT 1 FROM reading_tracker WHERE book_id = b.id) THEN 'tracking'
                              WHEN EXISTS (SELECT 1 FROM completed_books WHERE book_id = b.id) THEN 'completed'
                              WHEN EXISTS (SELECT 1 FROM abandoned_books WHERE book_id = b.id) THEN 'abandoned'
                              ELSE 'library'
                              END as status
                   FROM books b
                   WHERE LOWER(b.genre) = LOWER(?)
                   ORDER BY b.date_added DESC
                   """, (genre_name,))

    books = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return books


# ============================================================================
# RANDOM BOOK SELECTOR
# ============================================================================

def get_random_book(
        exclude_author: str = None,
        exclude_genre: str = None,
        max_page_count: int = None
) -> Optional[Dict]:
    """
    Get a random book from the library with optional filters.
    Only selects from books in 'library' status (unread).
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    where_conditions = ["""
        id NOT IN (SELECT book_id FROM reading_tracker)
        AND id NOT IN (SELECT book_id FROM completed_books)
        AND id NOT IN (SELECT book_id FROM abandoned_books)
    """]
    params = []

    if exclude_author:
        where_conditions.append("LOWER(author) != LOWER(?)")
        params.append(exclude_author)

    if exclude_genre:
        where_conditions.append("LOWER(genre) != LOWER(?)")
        params.append(exclude_genre)

    if max_page_count:
        where_conditions.append("page_count < ?")
        params.append(max_page_count)

    where_clause = " AND ".join(where_conditions)

    cursor.execute(f"""
        SELECT * FROM books
        WHERE {where_clause}
        ORDER BY RANDOM()
        LIMIT 1
    """, params)

    row = cursor.fetchone()
    conn.close()

    return dict(row) if row else None


def get_last_completed_book() -> Optional[Dict]:
    """Get the most recently completed book."""
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT b.*, cb.completion_date
                   FROM books b
                            JOIN completed_books cb ON b.id = cb.book_id
                   ORDER BY cb.completion_date DESC LIMIT 1
                   """)

    row = cursor.fetchone()
    conn.close()

    return dict(row) if row else None


def get_completed_books_by_author(author_name: str, year: int = None) -> List[Dict]:
    """Get completed books by an author, optionally filtered by year."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if year:
        cursor.execute("""
                       SELECT b.*,
                              cb.rating,
                              cb.completion_date
                       FROM books b
                                JOIN completed_books cb ON b.id = cb.book_id
                       WHERE LOWER(b.author) = LOWER(?)
                         AND strftime('%Y', cb.completion_date) = ?
                       ORDER BY cb.completion_date DESC
                       """, (author_name, str(year)))
    else:
        cursor.execute("""
                       SELECT b.*,
                              cb.rating,
                              cb.completion_date
                       FROM books b
                                JOIN completed_books cb ON b.id = cb.book_id
                       WHERE LOWER(b.author) = LOWER(?)
                       ORDER BY cb.completion_date DESC
                       """, (author_name,))

    books = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return books


def get_completed_books_by_genre(genre_name: str, year: int = None) -> List[Dict]:
    """Get completed books by genre, optionally filtered by year."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if year:
        cursor.execute("""
                       SELECT b.*,
                              cb.rating,
                              cb.completion_date
                       FROM books b
                                JOIN completed_books cb ON b.id = cb.book_id
                       WHERE LOWER(b.genre) = LOWER(?)
                         AND strftime('%Y', cb.completion_date) = ?
                       ORDER BY cb.completion_date DESC
                       """, (genre_name, str(year)))
    else:
        cursor.execute("""
                       SELECT b.*,
                              cb.rating,
                              cb.completion_date
                       FROM books b
                                JOIN completed_books cb ON b.id = cb.book_id
                       WHERE LOWER(b.genre) = LOWER(?)
                       ORDER BY cb.completion_date DESC
                       """, (genre_name,))

    books = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return books


def get_completed_books_by_series(series_name: str, year: int = None) -> List[Dict]:
    """Get completed books in a series, optionally filtered by year."""
    conn = get_db_connection()
    cursor = conn.cursor()

    if year:
        cursor.execute("""
                       SELECT b.*,
                              cb.rating,
                              cb.completion_date
                       FROM books b
                                JOIN completed_books cb ON b.id = cb.book_id
                       WHERE LOWER(b.series) = LOWER(?)
                         AND strftime('%Y', cb.completion_date) = ?
                       ORDER BY b.series_number
                       """, (series_name, str(year)))
    else:
        cursor.execute("""
                       SELECT b.*,
                              cb.rating,
                              cb.completion_date
                       FROM books b
                                JOIN completed_books cb ON b.id = cb.book_id
                       WHERE LOWER(b.series) = LOWER(?)
                       ORDER BY b.series_number
                       """, (series_name,))

    books = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return books