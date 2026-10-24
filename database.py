import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any

DATABASE_NAME = "pybooklet.db"

def get_db_connection():
    conn = sqlite3.connect(DATABASE_NAME)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema():
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(books)")
    columns = {row["name"] for row in cursor.fetchall()}

    if "isbn" not in columns:
        cursor.execute("ALTER TABLE books ADD COLUMN isbn TEXT")

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_books_isbn ON books(isbn)")

    conn.commit()
    conn.close()

def get_all_books(
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "date_added",
        search: str = None
) -> tuple[List[Dict], int]:
    conn = get_db_connection()
    cursor = conn.cursor()
    where_conditions = []
    params = []

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

    count_query = f"SELECT COUNT(*) as count FROM books WHERE {where_clause}"
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()["count"]

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
        genre: str = None,
        isbn: str = None
) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   INSERT INTO books (cover_url, title, series, series_number, author, page_count, synopsis, genre, isbn)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   """, (cover_url, title, series, series_number, author, page_count, synopsis, genre, isbn))

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
        genre: str = None,
        isbn: str = None
) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
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
    if isbn is not None:
        updates.append("isbn = ?")
        params.append(isbn)

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


def get_book_by_isbn(isbn: str) -> Optional[Dict]:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM books WHERE isbn = ?", (isbn,))
    row = cursor.fetchone()

    conn.close()
    return dict(row) if row else None


def delete_book(book_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM reading_sessions WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM tbr_list_books WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM books WHERE id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success

def get_book_status(book_id: int) -> str:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM books WHERE id = ?", (book_id,))
    if not cursor.fetchone():
        conn.close()
        return "unknown"

    cursor.execute("SELECT id FROM reading_tracker WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "tracking"

    cursor.execute("SELECT id FROM completed_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "completed"

    cursor.execute("SELECT id FROM abandoned_books WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return "abandoned"

    conn.close()
    return "library"

def get_tracked_books() -> List[Dict]:
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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))

    cursor.execute("SELECT id FROM reading_tracker WHERE book_id = ?", (book_id,))
    if cursor.fetchone():
        conn.close()
        return False

    cursor.execute("""
                   INSERT INTO reading_tracker (book_id, current_page)
                   VALUES (?, ?)
                   """, (book_id, current_page))

    conn.commit()
    conn.close()

    return True


def update_tracker_progress(book_id: int, current_page: int) -> bool:
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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success

def get_completed_books(
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "completion_date",
        search: str = None,
        year: int = None,
        rating: int = None
) -> tuple[List[Dict], int]:
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

    count_query = f"""
        SELECT COUNT(*) as count 
        FROM books b
        JOIN completed_books cb ON b.id = cb.book_id
        WHERE {where_clause}
    """
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()["count"]

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
    conn = get_db_connection()
    cursor = conn.cursor()

    if not start_date:
        cursor.execute("SELECT start_date FROM reading_tracker WHERE book_id = ?", (book_id,))
        row = cursor.fetchone()
        if row:
            start_date = row["start_date"]

    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))

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
    conn = get_db_connection()
    cursor = conn.cursor()

    updates = []
    params = []

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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success

def get_abandoned_books(
        limit: int = 50,
        offset: int = 0,
        sort_by: str = "abandonment_date",
        search: str = None
) -> tuple[List[Dict], int]:
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

    count_query = f"""
        SELECT COUNT(*) as count 
        FROM books b
        JOIN abandoned_books ab ON b.id = ab.book_id
        WHERE {where_clause}
    """
    cursor.execute(count_query, params)
    total_count = cursor.fetchone()["count"]

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
    conn = get_db_connection()
    cursor = conn.cursor()

    if not start_date:
        cursor.execute("SELECT start_date FROM reading_tracker WHERE book_id = ?", (book_id,))
        row = cursor.fetchone()
        if row:
            start_date = row["start_date"]

    cursor.execute("DELETE FROM reading_tracker WHERE book_id = ?", (book_id,))
    cursor.execute("DELETE FROM completed_books WHERE book_id = ?", (book_id,))

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
    conn = get_db_connection()
    cursor = conn.cursor()

    updates = []
    params = []

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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM abandoned_books WHERE book_id = ?", (book_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success

def add_reading_session(
        book_id: int,
        start_time: str,
        end_time: str,
        duration_seconds: int,
        start_page: int,
        end_page: int
) -> int:
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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT COUNT(*) as count
                   FROM reading_sessions
                   WHERE book_id = ?
                   """, (book_id,))
    total_count = cursor.fetchone()["count"]

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
    conn = get_db_connection()
    cursor = conn.cursor()

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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT COUNT(*) as count
                   FROM reading_sessions
                   """)
    total_count = cursor.fetchone()["count"]

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
    conn = get_db_connection()
    cursor = conn.cursor()

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
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM reading_sessions WHERE id = ?", (session_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success

def get_dashboard_stats() -> Dict:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT COUNT(*) as count
                   FROM books
                   WHERE id NOT IN (SELECT book_id FROM reading_tracker)
                     AND id NOT IN (SELECT book_id FROM completed_books)
                     AND id NOT IN (SELECT book_id FROM abandoned_books)
                   """)
    library_count = cursor.fetchone()["count"]

    cursor.execute("SELECT COUNT(*) as count FROM books")
    total_books = cursor.fetchone()["count"]

    cursor.execute("SELECT COUNT(*) as count FROM completed_books")
    completed_count = cursor.fetchone()["count"]

    cursor.execute("SELECT COUNT(*) as count FROM abandoned_books")
    abandoned_count = cursor.fetchone()["count"]

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
    conn = get_db_connection()
    cursor = conn.cursor()

    if year:
        cursor.execute("""
                       SELECT COUNT(*) as count
                       FROM completed_books
                       WHERE strftime('%Y', completion_date) = ?
                       """, (str(year),))
    else:
        cursor.execute("SELECT COUNT(*) as count FROM completed_books")

    completed_count = cursor.fetchone()["count"]

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

    if year:
        cursor.execute("""
                       SELECT COUNT(*) as count
                       FROM abandoned_books
                       WHERE strftime('%Y', abandonment_date) = ?
                       """, (str(year),))
    else:
        cursor.execute("SELECT COUNT(*) as count FROM abandoned_books")

    abandoned_count = cursor.fetchone()["count"]

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


def get_available_years() -> List[int]:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT DISTINCT strftime('%Y', completion_date) as year
                   FROM completed_books
                   WHERE completion_date IS NOT NULL
                   ORDER BY year DESC
                   """)
    years = [int(row['year']) for row in cursor.fetchall()]

    conn.close()
    return years


def get_monthly_reading_data(year: int) -> List[Dict]:
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

def get_random_book(
        exclude_author: str = None,
        exclude_genre: str = None,
        max_page_count: int = None,
        genre: str = None,
        author: str = None
) -> Optional[Dict]:

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

    if genre:
        where_conditions.append("LOWER(genre) = LOWER(?)")
        params.append(genre)

    if author:
        where_conditions.append("LOWER(author) = LOWER(?)")
        params.append(author)

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


def get_random_filter_options() -> Dict[str, List[str]]:
    conn = get_db_connection()
    cursor = conn.cursor()

    unread_clause = """
        id NOT IN (SELECT book_id FROM reading_tracker)
        AND id NOT IN (SELECT book_id FROM completed_books)
        AND id NOT IN (SELECT book_id FROM abandoned_books)
    """

    cursor.execute(f"""
        SELECT DISTINCT genre FROM books
        WHERE {unread_clause} AND genre IS NOT NULL AND genre != ''
        ORDER BY genre COLLATE NOCASE
    """)
    genres = [row['genre'] for row in cursor.fetchall()]

    cursor.execute(f"""
        SELECT DISTINCT author FROM books
        WHERE {unread_clause} AND author IS NOT NULL AND author != ''
        ORDER BY author COLLATE NOCASE
    """)
    authors = [row['author'] for row in cursor.fetchall()]

    conn.close()

    return {"genres": genres, "authors": authors}


def pick_random_book(genre: str = None, author: str = None) -> Optional[Dict]:
    last_completed = get_last_completed_book()

    exclude_author = None
    exclude_genre = None
    max_page_count = None

    if last_completed:
        if not author:
            exclude_author = last_completed.get('author')
        if not genre:
            exclude_genre = last_completed.get('genre')
        if not genre and not author and (last_completed.get('page_count') or 0) > 800:
            max_page_count = 600

    return get_random_book(
        exclude_author=exclude_author,
        exclude_genre=exclude_genre,
        max_page_count=max_page_count,
        genre=genre,
        author=author
    )


def get_last_completed_book() -> Optional[Dict]:
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

def create_tbr_list(name: str, description: str = None) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   INSERT INTO tbr_lists (name, description, date_created)
                   VALUES (?, ?, ?)
                   """, (name, description, datetime.now().date().isoformat()))

    list_id = cursor.lastrowid
    conn.commit()
    conn.close()

    return list_id


def get_all_tbr_lists() -> List[Dict]:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT tl.*,
                          COUNT(tlb.id) as book_count
                   FROM tbr_lists tl
                            LEFT JOIN tbr_list_books tlb ON tl.id = tlb.list_id
                   GROUP BY tl.id
                   ORDER BY tl.date_created DESC
                   """)

    lists = [dict(row) for row in cursor.fetchall()]
    conn.close()

    return lists


def get_tbr_list_by_id(list_id: int) -> Optional[Dict]:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT * FROM tbr_lists WHERE id = ?", (list_id,))
    row = cursor.fetchone()

    conn.close()
    return dict(row) if row else None


def update_tbr_list(list_id: int, name: str = None, description: str = None) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    updates = []
    params = []

    if name is not None:
        updates.append("name = ?")
        params.append(name)
    if description is not None:
        updates.append("description = ?")
        params.append(description)

    if not updates:
        conn.close()
        return False

    params.append(list_id)
    query = f"UPDATE tbr_lists SET {', '.join(updates)} WHERE id = ?"

    cursor.execute(query, params)
    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


def delete_tbr_list(list_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM tbr_list_books WHERE list_id = ?", (list_id,))

    cursor.execute("DELETE FROM tbr_lists WHERE id = ?", (list_id,))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


def get_books_in_tbr_list(list_id: int) -> List[Dict]:
    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        SELECT 
            b.*,
            tlb.date_added as list_date_added,
            tlb.position,
            CASE
                WHEN EXISTS (SELECT 1 FROM reading_tracker WHERE book_id = b.id) THEN 'tracking'
                WHEN EXISTS (SELECT 1 FROM completed_books WHERE book_id = b.id) THEN 'completed'
                WHEN EXISTS (SELECT 1 FROM abandoned_books WHERE book_id = b.id) THEN 'abandoned'
                ELSE 'library'
            END as status
        FROM tbr_list_books tlb
        JOIN books b ON tlb.book_id = b.id
        WHERE tlb.list_id = ?
        ORDER BY tlb.position ASC
    """

    cursor.execute(query, (list_id,))
    books = [dict(row) for row in cursor.fetchall()]

    conn.close()
    return books


def add_book_to_tbr_list(book_id: int, list_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM tbr_list_books WHERE book_id = ?", (book_id,))

    cursor.execute("""
                   SELECT COALESCE(MAX(position), -1) + 1 as next_pos
                   FROM tbr_list_books
                   WHERE list_id = ?
                   """, (list_id,))

    next_position = cursor.fetchone()["next_pos"]

    cursor.execute("""
                   INSERT INTO tbr_list_books (list_id, book_id, date_added, position)
                   VALUES (?, ?, ?, ?)
                   """, (list_id, book_id, datetime.now().date().isoformat(), next_position))

    conn.commit()
    conn.close()

    return True


def move_book_up(book_id: int, list_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT position
                   FROM tbr_list_books
                   WHERE book_id = ?
                     AND list_id = ?
                   """, (book_id, list_id))

    result = cursor.fetchone()
    if not result:
        conn.close()
        return False

    current_pos = result["position"]

    if current_pos == 0:
        conn.close()
        return False

    cursor.execute("""
                   SELECT book_id
                   FROM tbr_list_books
                   WHERE list_id = ?
                     AND position = ?
                   """, (list_id, current_pos - 1))

    above_book = cursor.fetchone()
    if not above_book:
        conn.close()
        return False

    above_book_id = above_book["book_id"]

    cursor.execute("""
                   UPDATE tbr_list_books
                   SET position = ?
                   WHERE book_id = ?
                     AND list_id = ?
                   """, (current_pos, above_book_id, list_id))

    cursor.execute("""
                   UPDATE tbr_list_books
                   SET position = ?
                   WHERE book_id = ?
                     AND list_id = ?
                   """, (current_pos - 1, book_id, list_id))

    conn.commit()
    conn.close()

    return True


def move_book_down(book_id: int, list_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT position
                   FROM tbr_list_books
                   WHERE book_id = ?
                     AND list_id = ?
                   """, (book_id, list_id))

    result = cursor.fetchone()
    if not result:
        conn.close()
        return False

    current_pos = result["position"]

    cursor.execute("""
                   SELECT MAX(position) as max_pos
                   FROM tbr_list_books
                   WHERE list_id = ?
                   """, (list_id,))

    max_pos = cursor.fetchone()["max_pos"]

    if current_pos >= max_pos:
        conn.close()
        return False

    cursor.execute("""
                   SELECT book_id
                   FROM tbr_list_books
                   WHERE list_id = ?
                     AND position = ?
                   """, (list_id, current_pos + 1))

    below_book = cursor.fetchone()
    if not below_book:
        conn.close()
        return False

    below_book_id = below_book["book_id"]

    cursor.execute("""
                   UPDATE tbr_list_books
                   SET position = ?
                   WHERE book_id = ?
                     AND list_id = ?
                   """, (current_pos, below_book_id, list_id))

    cursor.execute("""
                   UPDATE tbr_list_books
                   SET position = ?
                   WHERE book_id = ?
                     AND list_id = ?
                   """, (current_pos + 1, book_id, list_id))

    conn.commit()
    conn.close()

    return True

def remove_book_from_tbr_list(book_id: int, list_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   DELETE
                   FROM tbr_list_books
                   WHERE book_id = ?
                     AND list_id = ?
                   """, (book_id, list_id))

    conn.commit()
    success = cursor.rowcount > 0
    conn.close()

    return success


def get_book_tbr_list(book_id: int) -> Optional[Dict]:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT tl.*
                   FROM tbr_lists tl
                            JOIN tbr_list_books tlb ON tl.id = tlb.list_id
                   WHERE tlb.book_id = ?
                   """, (book_id,))

    row = cursor.fetchone()
    conn.close()

    return dict(row) if row else None


def get_tbr_list_count(list_id: int) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
                   SELECT COUNT(*) as count
                   FROM tbr_list_books
                   WHERE list_id = ?
                   """, (list_id,))

    count = cursor.fetchone()["count"]
    conn.close()

    return count