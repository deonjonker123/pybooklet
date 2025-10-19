from fastapi import FastAPI, Request, Form, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from typing import Optional
import database as db

app = FastAPI(title="PYBooklet")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Setup templates
templates = Jinja2Templates(directory="templates")


# ============================================================================
# DASHBOARD
# ============================================================================

@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """Main dashboard page."""
    stats = db.get_dashboard_stats()
    currently_reading = db.get_tracked_books()

    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "stats": stats,
        "currently_reading": currently_reading
    })


# ============================================================================
# LIBRARY ROUTES
# ============================================================================

@app.get("/library", response_class=HTMLResponse)
async def library_page(
        request: Request,
        page: int = 1,
        limit: int = 50,
        sort_by: str = "date_added",
        search: Optional[str] = None
):
    """Library page with all unread books."""
    offset = (page - 1) * limit
    books, total_count = db.get_all_books(limit=limit, offset=offset, sort_by=sort_by, search=search)

    total_pages = (total_count + limit - 1) // limit

    return templates.TemplateResponse("library.html", {
        "request": request,
        "books": books,
        "current_page": page,
        "total_pages": total_pages,
        "limit": limit,
        "sort_by": sort_by,
        "search": search or "",
        "total_count": total_count
    })


@app.post("/library/add")
async def add_book(
        title: str = Form(...),
        author: str = Form(...),
        page_count: int = Form(...),
        cover_url: Optional[str] = Form(None),
        series: Optional[str] = Form(None),
        series_number: Optional[float] = Form(None),
        synopsis: Optional[str] = Form(None),
        genre: Optional[str] = Form(None)
):
    """Add a new book to the library."""
    book_id = db.add_book(
        title=title,
        author=author,
        page_count=page_count,
        cover_url=cover_url,
        series=series,
        series_number=series_number,
        synopsis=synopsis,
        genre=genre
    )

    return RedirectResponse(url="/library", status_code=303)


@app.post("/library/update/{book_id}")
async def update_book(
        book_id: int,
        title: Optional[str] = Form(None),
        author: Optional[str] = Form(None),
        page_count: Optional[int] = Form(None),
        cover_url: Optional[str] = Form(None),
        series: Optional[str] = Form(None),
        series_number: Optional[float] = Form(None),
        synopsis: Optional[str] = Form(None),
        genre: Optional[str] = Form(None)
):
    """Update book details."""
    success = db.update_book(
        book_id=book_id,
        title=title,
        author=author,
        page_count=page_count,
        cover_url=cover_url,
        series=series,
        series_number=series_number,
        synopsis=synopsis,
        genre=genre
    )

    if not success:
        raise HTTPException(status_code=404, detail="Book not found")

    return RedirectResponse(url="/library", status_code=303)


@app.post("/library/read/{book_id}")
async def move_to_tracker(book_id: int):
    """Move a book from library to reading tracker."""
    success = db.add_to_tracker(book_id)

    if not success:
        raise HTTPException(status_code=400, detail="Could not add book to tracker")

    return RedirectResponse(url="/tracker", status_code=303)


@app.post("/library/delete/{book_id}")
async def delete_book(book_id: int):
    """Permanently delete a book."""
    success = db.delete_book(book_id)

    if not success:
        raise HTTPException(status_code=404, detail="Book not found")

    return RedirectResponse(url="/library", status_code=303)


# ============================================================================
# LIBRARY SEARCH/FILTER (HTMX ENDPOINTS)
# ============================================================================

@app.get("/library/search", response_class=HTMLResponse)
async def library_search(
        request: Request,
        search: str = "",
        sort_by: str = "date_added",
        limit: int = 50
):
    """HTMX endpoint for real-time library search."""
    books, total_count = db.get_all_books(limit=limit, offset=0, sort_by=sort_by, search=search)

    return templates.TemplateResponse("partials/library_table.html", {
        "request": request,
        "books": books,
        "total_count": total_count
    })


# ============================================================================
# READING TRACKER ROUTES
# ============================================================================

@app.get("/tracker", response_class=HTMLResponse)
async def tracker_page(request: Request):
    """Reading tracker page with currently tracked books."""
    books = db.get_tracked_books()

    return templates.TemplateResponse("tracker.html", {
        "request": request,
        "books": books
    })


@app.post("/tracker/update-progress/{book_id}")
async def update_progress(
        book_id: int,
        current_page: int = Form(...)
):
    """Update the current page progress for a tracked book."""
    success = db.update_tracker_progress(book_id, current_page)

    if not success:
        raise HTTPException(status_code=404, detail="Book not found in tracker")

    return RedirectResponse(url="/tracker", status_code=303)


@app.post("/tracker/complete/{book_id}")
async def complete_tracked_book(
        book_id: int,
        rating: Optional[int] = Form(None),
        review: Optional[str] = Form(None)
):
    """Complete a tracked book and move to completed."""
    success = db.complete_book(book_id, rating=rating, review=review)

    if not success:
        raise HTTPException(status_code=400, detail="Could not complete book")

    return RedirectResponse(url="/completed", status_code=303)


@app.post("/tracker/abandon/{book_id}")
async def abandon_tracked_book(
        book_id: int,
        page_at_abandonment: int = Form(...),
        reason: Optional[str] = Form(None)
):
    """Abandon a tracked book and move to abandoned."""
    success = db.abandon_book(book_id, page_at_abandonment=page_at_abandonment, reason=reason)

    if not success:
        raise HTTPException(status_code=400, detail="Could not abandon book")

    return RedirectResponse(url="/abandoned", status_code=303)


@app.post("/tracker/remove/{book_id}")
async def remove_from_tracker(book_id: int):
    """Remove a book from tracker and return to library."""
    success = db.remove_from_tracker(book_id)

    if not success:
        raise HTTPException(status_code=404, detail="Book not found in tracker")

    return RedirectResponse(url="/library", status_code=303)


# ============================================================================
# READING SESSION ROUTES (TIMER)
# ============================================================================

@app.post("/tracker/session/start/{book_id}")
async def start_session(
        book_id: int,
        start_time: str = Form(...),
        start_page: int = Form(...)
):
    """
    Start a reading session (called when timer modal opens).
    This just validates the book exists - actual session saved on stop.
    """
    book = db.get_book_by_id(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    # Return success - session will be saved on stop
    return {"status": "started", "book_id": book_id}


@app.post("/tracker/session/stop/{book_id}")
async def stop_session(
        book_id: int,
        start_time: str = Form(...),
        end_time: str = Form(...),
        duration_seconds: int = Form(...),
        start_page: int = Form(...),
        end_page: int = Form(...)
):
    """Stop a reading session and save it."""
    # Save the session
    session_id = db.add_reading_session(
        book_id=book_id,
        start_time=start_time,
        end_time=end_time,
        duration_seconds=duration_seconds,
        start_page=start_page,
        end_page=end_page
    )

    # Update tracker progress to end_page
    db.update_tracker_progress(book_id, end_page)

    return RedirectResponse(url="/tracker", status_code=303)


# ============================================================================
# COMPLETED BOOKS ROUTES
# ============================================================================

@app.get("/completed", response_class=HTMLResponse)
async def completed_page(
        request: Request,
        page: int = 1,
        limit: int = 50,
        sort_by: str = "completion_date",
        search: Optional[str] = None,
        rating: Optional[int] = None
):
    """Completed books page."""
    offset = (page - 1) * limit

    # If rating filter is applied, we need custom query
    # For now, get all and filter in template or add to db function
    books, total_count = db.get_completed_books(
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        search=search
    )

    # Apply rating filter if provided
    if rating is not None:
        books = [b for b in books if b.get('rating') == rating]
        total_count = len(books)

    total_pages = (total_count + limit - 1) // limit

    return templates.TemplateResponse("completed.html", {
        "request": request,
        "books": books,
        "current_page": page,
        "total_pages": total_pages,
        "limit": limit,
        "sort_by": sort_by,
        "search": search or "",
        "total_count": total_count,
        "rating_filter": rating
    })


@app.post("/completed/update/{book_id}")
async def update_completed_book(
        book_id: int,
        rating: Optional[int] = Form(None),
        review: Optional[str] = Form(None),
        start_date: Optional[str] = Form(None),
        completion_date: Optional[str] = Form(None)
):
    """Update completed book details."""
    # Convert empty strings to None
    review = None if (review == "" or review is None) else review
    start_date = None if (start_date == "" or start_date is None) else start_date
    completion_date = None if (completion_date == "" or completion_date is None) else completion_date

    success = db.update_completed_book(
        book_id=book_id,
        rating=rating,
        review=review,
        start_date=start_date,
        completion_date=completion_date
    )

    if not success:
        raise HTTPException(status_code=404, detail="Completed book not found")

    return RedirectResponse(url="/completed", status_code=303)


@app.post("/completed/remove/{book_id}")
async def remove_from_completed(book_id: int):
    """Remove a book from completed and return to library."""
    success = db.remove_from_completed(book_id)

    if not success:
        raise HTTPException(status_code=404, detail="Book not found in completed")

    return RedirectResponse(url="/library", status_code=303)


@app.get("/completed/search", response_class=HTMLResponse)
async def completed_search(
        request: Request,
        search: str = "",
        sort_by: str = "completion_date",
        limit: int = 50
):
    """HTMX endpoint for real-time completed books search."""
    books, total_count = db.get_completed_books(
        limit=limit,
        offset=0,
        sort_by=sort_by,
        search=search
    )

    return templates.TemplateResponse("partials/completed_table.html", {
        "request": request,
        "books": books,
        "total_count": total_count
    })


# ============================================================================
# ABANDONED BOOKS ROUTES
# ============================================================================

@app.get("/abandoned", response_class=HTMLResponse)
async def abandoned_page(
        request: Request,
        page: int = 1,
        limit: int = 50,
        sort_by: str = "abandonment_date",
        search: Optional[str] = None
):
    """Abandoned books page."""
    offset = (page - 1) * limit
    books, total_count = db.get_abandoned_books(
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        search=search
    )

    total_pages = (total_count + limit - 1) // limit

    return templates.TemplateResponse("abandoned.html", {
        "request": request,
        "books": books,
        "current_page": page,
        "total_pages": total_pages,
        "limit": limit,
        "sort_by": sort_by,
        "search": search or "",
        "total_count": total_count
    })


@app.post("/abandoned/update/{book_id}")
async def update_abandoned_book(
        book_id: int,
        page_at_abandonment: Optional[int] = Form(None),
        reason: Optional[str] = Form(None),
        start_date: Optional[str] = Form(None),
        abandonment_date: Optional[str] = Form(None)
):
    """Update abandoned book details."""
    # Convert empty strings to None
    reason = None if (reason == "" or reason is None) else reason
    start_date = None if (start_date == "" or start_date is None) else start_date
    abandonment_date = None if (abandonment_date == "" or abandonment_date is None) else abandonment_date

    success = db.update_abandoned_book(
        book_id=book_id,
        page_at_abandonment=page_at_abandonment,
        reason=reason,
        start_date=start_date,
        abandonment_date=abandonment_date
    )

    if not success:
        raise HTTPException(status_code=404, detail="Abandoned book not found")

    return RedirectResponse(url="/abandoned", status_code=303)


@app.post("/abandoned/remove/{book_id}")
async def remove_from_abandoned(book_id: int):
    """Remove a book from abandoned and return to library."""
    success = db.remove_from_abandoned(book_id)

    if not success:
        raise HTTPException(status_code=404, detail="Book not found in abandoned")

    return RedirectResponse(url="/library", status_code=303)


@app.get("/abandoned/search", response_class=HTMLResponse)
async def abandoned_search(
        request: Request,
        search: str = "",
        sort_by: str = "abandonment_date",
        limit: int = 50
):
    """HTMX endpoint for real-time abandoned books search."""
    books, total_count = db.get_abandoned_books(
        limit=limit,
        offset=0,
        sort_by=sort_by,
        search=search
    )

    return templates.TemplateResponse("partials/abandoned_table.html", {
        "request": request,
        "books": books,
        "total_count": total_count
    })


# ============================================================================
# SESSIONS PAGE ROUTES
# ============================================================================

@app.get("/sessions/{book_id}", response_class=HTMLResponse)
async def sessions_page(
        request: Request,
        book_id: int,  # Keep for URL structure, but don't use for filtering
        week_start: Optional[str] = None,
        page: int = 1,
        limit: int = 50
):
    """
    Sessions page showing ALL reading sessions across ALL books.
    book_id is kept in URL for consistency but data is not filtered by it.
    """
    from datetime import datetime, timedelta

    # Get the book details (for header display only)
    book = db.get_book_by_id(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    # Get book status (for header display only)
    status = db.get_book_status(book_id)

    # Determine week_start (default to current week's Sunday)
    if not week_start:
        today = datetime.now()
        days_since_sunday = today.weekday() + 1 if today.weekday() != 6 else 0
        current_sunday = today - timedelta(days=days_since_sunday)
        week_start = current_sunday.date().isoformat()

    # Get weekly sessions for the graph - ACROSS ALL BOOKS
    weekly_sessions = db.get_all_weekly_sessions(week_start)

    # Calculate weekly stats - ALL BOOKS
    total_pages = sum(s['pages_read'] for s in weekly_sessions)
    total_duration = sum(s['duration_seconds'] for s in weekly_sessions)
    reading_days = len(set(s['session_date'] for s in weekly_sessions))

    avg_pages_per_hour = 0
    if total_duration > 0:
        avg_pages_per_hour = round((total_pages / total_duration) * 3600, 1)

    # Get previous week stats for comparison - ALL BOOKS
    prev_week_start = (datetime.fromisoformat(week_start) - timedelta(days=7)).date().isoformat()
    prev_weekly_sessions = db.get_all_weekly_sessions(prev_week_start)
    prev_total_pages = sum(s['pages_read'] for s in prev_weekly_sessions)
    prev_total_duration = sum(s['duration_seconds'] for s in prev_weekly_sessions)
    prev_reading_days = len(set(s['session_date'] for s in prev_weekly_sessions))
    prev_avg_pages_per_hour = 0
    if prev_total_duration > 0:
        prev_avg_pages_per_hour = round((prev_total_pages / prev_total_duration) * 3600, 1)

    # Calculate changes
    pages_change = total_pages - prev_total_pages
    days_change = reading_days - prev_reading_days
    pace_change = round(avg_pages_per_hour - prev_avg_pages_per_hour, 1)

    # Get all sessions for table (paginated) - ACROSS ALL BOOKS
    offset = (page - 1) * limit
    all_sessions, total_count = db.get_all_sessions(limit=limit, offset=offset)
    total_pages_pagination = (total_count + limit - 1) // limit

    # Calculate next and previous week dates
    next_week = (datetime.fromisoformat(week_start) + timedelta(days=7)).date().isoformat()
    prev_week = prev_week_start

    return templates.TemplateResponse("sessions.html", {
        "request": request,
        "book": book,
        "status": status,
        "week_start": week_start,
        "next_week": next_week,
        "prev_week": prev_week,
        "weekly_sessions": weekly_sessions,
        "total_pages": total_pages,
        "total_duration": total_duration,
        "reading_days": reading_days,
        "avg_pages_per_hour": avg_pages_per_hour,
        "pages_change": pages_change,
        "days_change": days_change,
        "pace_change": pace_change,
        "all_sessions": all_sessions,
        "current_page": page,
        "total_pages_pagination": total_pages_pagination,
        "limit": limit,
        "total_session_count": total_count
    })


@app.post("/sessions/delete/{session_id}")
async def delete_session(session_id: int, book_id: int = Form(...)):
    """Delete a reading session."""
    success = db.delete_session(session_id)

    if not success:
        raise HTTPException(status_code=404, detail="Session not found")

    return RedirectResponse(url=f"/sessions/{book_id}", status_code=303)


# ============================================================================
# STATISTICS PAGE ROUTES
# ============================================================================

@app.get("/statistics", response_class=HTMLResponse)
async def statistics_page(request: Request, year: Optional[int] = None):
    """Statistics page with yearly and all-time stats."""
    from datetime import datetime

    # Default to current year
    current_year = datetime.now().year

    # If year is explicitly 0, that means "All Time"
    # If year is None, default to current year
    if year == 0:
        selected_year = 0
    elif year is None:
        selected_year = current_year
    else:
        selected_year = year

    # Get stats for selected year (0 or None = all time)
    stats = db.get_year_stats(year=selected_year if selected_year != 0 else None)

    # Get monthly reading data for line graph (only if not "All Time")
    monthly_data = []
    if selected_year != 0:
        monthly_data = db.get_monthly_reading_data(selected_year)

    # Get top authors
    top_authors = db.get_top_authors(year=selected_year if selected_year != 0 else None)

    # Get top genres
    top_genres = db.get_top_genres(year=selected_year if selected_year != 0 else None)

    # Get rating distribution
    rating_dist = db.get_rating_distribution(year=selected_year if selected_year != 0 else None)

    # Get available years for dropdown
    conn = db.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
                   SELECT DISTINCT strftime('%Y', completion_date) as year
                   FROM completed_books
                   WHERE completion_date IS NOT NULL
                   ORDER BY year DESC
                   """)
    available_years = [int(row['year']) for row in cursor.fetchall()]
    conn.close()

    return templates.TemplateResponse("statistics.html", {
        "request": request,
        "selected_year": selected_year,
        "current_year": current_year,
        "available_years": available_years,
        "stats": stats,
        "monthly_data": monthly_data,
        "top_authors": top_authors,
        "top_genres": top_genres,
        "rating_dist": rating_dist
    })


# ============================================================================
# RANDOM BOOK PAGE ROUTES
# ============================================================================

@app.get("/random", response_class=HTMLResponse)
async def random_book_page(request: Request):
    """Random book selector page."""
    return templates.TemplateResponse("random.html", {
        "request": request,
        "book": None
    })


@app.post("/random/select", response_class=HTMLResponse)
async def select_random_book(request: Request):
    """Select a random book with smart filtering based on last completed."""
    # Get last completed book for smart rules
    last_completed = db.get_last_completed_book()

    exclude_author = None
    exclude_genre = None
    max_page_count = None

    if last_completed:
        # Apply smart rules
        exclude_author = last_completed.get('author')
        exclude_genre = last_completed.get('genre')

        # If last book was > 800 pages, select book < 600 pages
        if last_completed.get('page_count', 0) > 800:
            max_page_count = 600

    # Get random book
    book = db.get_random_book(
        exclude_author=exclude_author,
        exclude_genre=exclude_genre,
        max_page_count=max_page_count
    )

    # Get book status if found
    status = None
    if book:
        status = db.get_book_status(book['id'])

    return templates.TemplateResponse("partials/random_book.html", {
        "request": request,
        "book": book,
        "status": status
    })


# ============================================================================
# BOOK DETAILS PAGE
# ============================================================================

@app.get("/book/{book_id}", response_class=HTMLResponse)
async def book_details_page(request: Request, book_id: int):
    """Book details page."""
    book = db.get_book_by_id(book_id)

    if not book:
        raise HTTPException(status_code=404, detail="Book not found")

    # Get book status
    status = db.get_book_status(book_id)

    # Check if book has session data
    sessions, _ = db.get_book_sessions(book_id, limit=1)
    has_sessions = len(sessions) > 0

    return templates.TemplateResponse("book_details.html", {
        "request": request,
        "book": book,
        "status": status,
        "has_sessions": has_sessions
    })


# ============================================================================
# SERIES/AUTHOR/GENRE FILTERED PAGES
# ============================================================================

@app.get("/series/{series_name}", response_class=HTMLResponse)
async def series_page(
        request: Request,
        series_name: str,
        page: int = 1,
        limit: int = 50,
        sort_by: str = "series_number",
        year: Optional[int] = None,
        completed_only: bool = False
):
    """Page showing all books in a series, optionally filtered by year for completed books."""
    # If year is provided OR completed_only flag is set, show only completed books
    if year is not None or completed_only:
        all_books = db.get_completed_books_by_series(series_name, year=year)
        # Add status for template
        for book in all_books:
            book['status'] = 'completed'
    else:
        # Show all books in series across all statuses
        all_books = db.get_books_by_series(series_name)

    # Apply sorting
    if sort_by == "series_number":
        all_books.sort(key=lambda x: x.get('series_number') or 0)
    elif sort_by == "title":
        all_books.sort(key=lambda x: x.get('title', ''))
    elif sort_by == "author":
        all_books.sort(key=lambda x: x.get('author', ''))
    elif sort_by == "page_count":
        all_books.sort(key=lambda x: x.get('page_count', 0), reverse=True)
    elif sort_by == "date_added":
        all_books.sort(key=lambda x: x.get('date_added', ''), reverse=True)

    # Pagination
    total_count = len(all_books)
    total_pages = (total_count + limit - 1) // limit
    offset = (page - 1) * limit
    books = all_books[offset:offset + limit]

    return templates.TemplateResponse("series.html", {
        "request": request,
        "series_name": series_name,
        "books": books,
        "current_page": page,
        "total_pages": total_pages,
        "limit": limit,
        "sort_by": sort_by,
        "total_count": total_count,
        "year": year
    })


@app.get("/author/{author_name}", response_class=HTMLResponse)
async def author_page(
        request: Request,
        author_name: str,
        page: int = 1,
        limit: int = 50,
        sort_by: str = "date_added",
        year: Optional[int] = None,
        completed_only: bool = False
):
    """Page showing all books by an author, optionally filtered by year for completed books."""
    # If year is provided OR completed_only flag is set, show only completed books
    if year is not None or completed_only:
        all_books = db.get_completed_books_by_author(author_name, year=year)
        # Add status for template
        for book in all_books:
            book['status'] = 'completed'
    else:
        # Show all books by author across all statuses
        all_books = db.get_books_by_author(author_name)

    # Apply sorting
    if sort_by == "title":
        all_books.sort(key=lambda x: x.get('title', ''))
    elif sort_by == "page_count":
        all_books.sort(key=lambda x: x.get('page_count', 0), reverse=True)
    elif sort_by == "date_added":
        all_books.sort(key=lambda x: x.get('date_added', ''), reverse=True)

    # Pagination
    total_count = len(all_books)
    total_pages = (total_count + limit - 1) // limit
    offset = (page - 1) * limit
    books = all_books[offset:offset + limit]

    return templates.TemplateResponse("author.html", {
        "request": request,
        "author_name": author_name,
        "books": books,
        "current_page": page,
        "total_pages": total_pages,
        "limit": limit,
        "sort_by": sort_by,
        "total_count": total_count,
        "year": year
    })


@app.get("/genre/{genre_name}", response_class=HTMLResponse)
async def genre_page(
        request: Request,
        genre_name: str,
        page: int = 1,
        limit: int = 50,
        sort_by: str = "date_added",
        year: Optional[int] = None,
        completed_only: bool = False
):
    """Page showing all books in a genre, optionally filtered by year for completed books."""
    # If year is provided OR completed_only flag is set, show only completed books
    if year is not None or completed_only:
        all_books = db.get_completed_books_by_genre(genre_name, year=year)
        # Add status for template
        for book in all_books:
            book['status'] = 'completed'
    else:
        # Show all books in genre across all statuses
        all_books = db.get_books_by_genre(genre_name)

    # Apply sorting
    if sort_by == "title":
        all_books.sort(key=lambda x: x.get('title', ''))
    elif sort_by == "author":
        all_books.sort(key=lambda x: x.get('author', ''))
    elif sort_by == "page_count":
        all_books.sort(key=lambda x: x.get('page_count', 0), reverse=True)
    elif sort_by == "date_added":
        all_books.sort(key=lambda x: x.get('date_added', ''), reverse=True)

    # Pagination
    total_count = len(all_books)
    total_pages = (total_count + limit - 1) // limit
    offset = (page - 1) * limit
    books = all_books[offset:offset + limit]

    return templates.TemplateResponse("genre.html", {
        "request": request,
        "genre_name": genre_name,
        "books": books,
        "current_page": page,
        "total_pages": total_pages,
        "limit": limit,
        "sort_by": sort_by,
        "total_count": total_count,
        "year": year
    })


# ============================================================================
# BACKUP & RESTORE ROUTES
# ============================================================================

@app.get("/backup")
async def backup_database():
    """Download database backup."""
    from fastapi.responses import FileResponse
    from datetime import datetime
    import os

    # Check if database exists
    if not os.path.exists(db.DATABASE_NAME):
        raise HTTPException(status_code=404, detail="Database not found")

    # Create backup filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"pybooklet_backup_{timestamp}.db"

    return FileResponse(
        path=db.DATABASE_NAME,
        filename=backup_filename,
        media_type="application/x-sqlite3"
    )

@app.get("/restore", response_class=HTMLResponse)
async def restore_page(request: Request):
    """Restore database page."""
    return templates.TemplateResponse("restore.html", {
        "request": request
    })


@app.post("/restore")
async def restore_database(file: UploadFile = File(...)):
    """Restore database from uploaded backup."""
    import shutil
    from fastapi import UploadFile, File

    # Validate file extension
    if not file.filename.endswith('.db'):
        raise HTTPException(status_code=400, detail="Invalid file type. Must be a .db file")

    # Create backup of current database first
    backup_path = f"{db.DATABASE_NAME}.backup_before_restore"
    shutil.copy2(db.DATABASE_NAME, backup_path)

    try:
        # Write uploaded file to database location
        with open(db.DATABASE_NAME, "wb") as f:
            shutil.copyfileobj(file.file, f)

        return RedirectResponse(url="/?restored=true", status_code=303)
    except Exception as e:
        # If restore fails, restore the backup
        shutil.copy2(backup_path, db.DATABASE_NAME)
        raise HTTPException(status_code=500, detail=f"Restore failed: {str(e)}")


# ============================================================================
# TBR LISTS ROUTES
# ============================================================================

@app.get("/tbr", response_class=HTMLResponse)
async def tbr_lists_page(request: Request):
    """Main TBR lists page showing all lists."""
    lists = db.get_all_tbr_lists()

    return templates.TemplateResponse("tbr.html", {
        "request": request,
        "lists": lists
    })


@app.post("/tbr/create")
async def create_tbr_list(
        name: str = Form(...),
        description: Optional[str] = Form(None)
):
    """Create a new TBR list."""
    list_id = db.create_tbr_list(name=name, description=description)
    return RedirectResponse(url="/tbr", status_code=303)


@app.post("/tbr/edit/{list_id}")
async def edit_tbr_list(
        list_id: int,
        name: str = Form(...),
        description: Optional[str] = Form(None)
):
    """Edit TBR list details."""
    success = db.update_tbr_list(list_id=list_id, name=name, description=description)

    if not success:
        raise HTTPException(status_code=404, detail="TBR list not found")

    return RedirectResponse(url="/tbr", status_code=303)


@app.post("/tbr/delete/{list_id}")
async def delete_tbr_list(list_id: int):
    """Delete a TBR list and all its book associations."""
    success = db.delete_tbr_list(list_id)

    if not success:
        raise HTTPException(status_code=404, detail="TBR list not found")

    return RedirectResponse(url="/tbr", status_code=303)


@app.get("/tbr/{list_id}", response_class=HTMLResponse)
async def tbr_list_page(request: Request, list_id: int):
    """View books in a specific TBR list."""
    # Get list details
    tbr_list = db.get_tbr_list_by_id(list_id)
    if not tbr_list:
        raise HTTPException(status_code=404, detail="TBR list not found")

    # Get books in list (no pagination, no search, no sort)
    books = db.get_books_in_tbr_list(list_id=list_id)

    return templates.TemplateResponse("tbr_list.html", {
        "request": request,
        "tbr_list": tbr_list,
        "books": books,
        "total_count": len(books)
    })


@app.post("/tbr/{list_id}/remove/{book_id}")
async def remove_book_from_tbr_list(list_id: int, book_id: int):
    """Remove a book from a TBR list."""
    success = db.remove_book_from_tbr_list(book_id=book_id, list_id=list_id)

    if not success:
        raise HTTPException(status_code=404, detail="Book not found in list")

    return RedirectResponse(url=f"/tbr/{list_id}", status_code=303)


@app.post("/tbr/add-book")
async def add_book_to_tbr(
        book_id: int = Form(...),
        list_id: int = Form(...),
        return_url: Optional[str] = Form(None)
):
    """Add a book to a TBR list (or move it from another list)."""
    success = db.add_book_to_tbr_list(book_id=book_id, list_id=list_id)

    if not success:
        raise HTTPException(status_code=400, detail="Failed to add book to list")

    # Return to the page they came from
    redirect_url = return_url if return_url else f"/book/{book_id}"
    return RedirectResponse(url=redirect_url, status_code=303)


@app.get("/api/tbr/lists")
async def get_tbr_lists_json():
    """API endpoint to get all TBR lists as JSON."""
    from fastapi.responses import JSONResponse

    lists = db.get_all_tbr_lists()

    return JSONResponse({
        "lists": lists
    })

@app.post("/tbr/{list_id}/move-up/{book_id}")
async def move_book_up_in_list(list_id: int, book_id: int):
    """Move a book up in the TBR list."""
    success = db.move_book_up(book_id=book_id, list_id=list_id)

    if not success:
        raise HTTPException(status_code=400, detail="Cannot move book up")

    return RedirectResponse(url=f"/tbr/{list_id}", status_code=303)


@app.post("/tbr/{list_id}/move-down/{book_id}")
async def move_book_down_in_list(list_id: int, book_id: int):
    """Move a book down in the TBR list."""
    success = db.move_book_down(book_id=book_id, list_id=list_id)

    if not success:
        raise HTTPException(status_code=400, detail="Cannot move book down")

    return RedirectResponse(url=f"/tbr/{list_id}", status_code=303)

@app.get("/tbr/book-status/{book_id}")
async def get_book_tbr_status(book_id: int):
    """Get which TBR list a book is currently on (for modal display)."""
    from fastapi.responses import JSONResponse

    tbr_list = db.get_book_tbr_list(book_id)

    return JSONResponse({
        "on_list": tbr_list is not None,
        "list": tbr_list
    })

# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)