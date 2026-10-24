from datetime import datetime, timedelta
from typing import Optional, List, Dict

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import database as db
import goodreads

router = APIRouter(prefix="/api", tags=["api"])

class BookIn(BaseModel):
    title: str
    author: str
    page_count: int
    cover_url: Optional[str] = None
    series: Optional[str] = None
    series_number: Optional[float] = None
    synopsis: Optional[str] = None
    genre: Optional[str] = None
    isbn: Optional[str] = None


class BookUpdate(BaseModel):
    title: Optional[str] = None
    author: Optional[str] = None
    page_count: Optional[int] = None
    cover_url: Optional[str] = None
    series: Optional[str] = None
    series_number: Optional[float] = None
    synopsis: Optional[str] = None
    genre: Optional[str] = None
    isbn: Optional[str] = None


class ProgressIn(BaseModel):
    current_page: int


class CompleteIn(BaseModel):
    rating: Optional[int] = None
    review: Optional[str] = None


class AbandonIn(BaseModel):
    page_at_abandonment: int
    reason: Optional[str] = None


class CompletedUpdate(BaseModel):
    rating: Optional[int] = None
    review: Optional[str] = None
    start_date: Optional[str] = None
    completion_date: Optional[str] = None


class AbandonedUpdate(BaseModel):
    page_at_abandonment: Optional[int] = None
    reason: Optional[str] = None
    start_date: Optional[str] = None
    abandonment_date: Optional[str] = None


class SessionIn(BaseModel):
    book_id: int
    start_time: str
    end_time: str
    duration_seconds: int
    start_page: int
    end_page: int


class RandomIn(BaseModel):
    genre: Optional[str] = None
    author: Optional[str] = None


class TbrListIn(BaseModel):
    name: str
    description: Optional[str] = None


class TbrListUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class TbrBookIn(BaseModel):
    book_id: int


def _page(items: List[Dict], total: int, page: int, limit: int) -> Dict:
    return {
        "items": items,
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit if limit else 1,
    }


def _blank_to_none(value: Optional[str]) -> Optional[str]:
    return None if value is None or value.strip() == "" else value


def _clean_isbn(value: Optional[str]) -> Optional[str]:
    if not _blank_to_none(value):
        return None
    isbn = goodreads.normalize_isbn(value)
    if not isbn:
        raise HTTPException(status_code=422, detail="Not a valid ISBN")
    return isbn


def _require_book(book_id: int) -> Dict:
    book = db.get_book_by_id(book_id)
    if not book:
        raise HTTPException(status_code=404, detail="Book not found")
    return book


def _current_week_start() -> str:
    today = datetime.now()
    days_since_sunday = today.weekday() + 1 if today.weekday() != 6 else 0
    return (today - timedelta(days=days_since_sunday)).date().isoformat()


def _week_summary(sessions: List[Dict]) -> Dict:
    total_pages = sum(s['pages_read'] for s in sessions)
    total_duration = sum(s['duration_seconds'] for s in sessions)
    reading_days = len(set(s['session_date'] for s in sessions))
    pages_per_hour = round((total_pages / total_duration) * 3600, 1) if total_duration > 0 else 0

    return {
        "total_pages": total_pages,
        "total_duration": total_duration,
        "reading_days": reading_days,
        "avg_pages_per_hour": pages_per_hour,
    }

@router.get("/dashboard")
async def dashboard():
    return {
        "stats": db.get_dashboard_stats(),
        "currently_reading": db.get_tracked_books(),
    }

@router.get("/books")
async def list_books(
        status: str = "library",
        page: int = 1,
        limit: int = 50,
        sort_by: Optional[str] = None,
        search: Optional[str] = None,
        year: Optional[int] = None,
        rating: Optional[int] = None
):
    offset = (page - 1) * limit

    if status == "library":
        books, total = db.get_all_books(limit=limit, offset=offset, sort_by=sort_by or "date_added", search=search)
    elif status == "completed":
        books, total = db.get_completed_books(
            limit=limit, offset=offset, sort_by=sort_by or "completion_date",
            search=search, year=year, rating=rating
        )
    elif status == "abandoned":
        books, total = db.get_abandoned_books(limit=limit, offset=offset, sort_by=sort_by or "abandonment_date", search=search)
    else:
        raise HTTPException(status_code=400, detail="status must be library, completed or abandoned")

    return _page(books, total, page, limit)


@router.get("/books/{book_id}")
async def get_book(book_id: int):
    book = _require_book(book_id)
    sessions, _ = db.get_book_sessions(book_id, limit=1)

    return {
        "book": book,
        "status": db.get_book_status(book_id),
        "tbr_list": db.get_book_tbr_list(book_id),
        "has_sessions": len(sessions) > 0,
    }


@router.post("/books", status_code=201)
async def add_book(data: BookIn):
    isbn = _clean_isbn(data.isbn)

    if isbn:
        existing = db.get_book_by_isbn(isbn)
        if existing:
            raise HTTPException(status_code=409, detail={
                "message": "This book is already in Booklet",
                "book_id": existing["id"],
            })

    book_id = db.add_book(
        title=data.title,
        author=data.author,
        page_count=data.page_count,
        cover_url=_blank_to_none(data.cover_url),
        series=_blank_to_none(data.series),
        series_number=data.series_number,
        synopsis=_blank_to_none(data.synopsis),
        genre=_blank_to_none(data.genre),
        isbn=isbn
    )

    return {"id": book_id}


@router.patch("/books/{book_id}")
async def update_book(book_id: int, data: BookUpdate):
    _require_book(book_id)
    fields = data.model_dump(exclude_unset=True)

    if "isbn" in fields:
        fields["isbn"] = _clean_isbn(fields["isbn"])

    if not db.update_book(book_id=book_id, **fields):
        raise HTTPException(status_code=400, detail="Nothing to update")

    return {"ok": True}


@router.delete("/books/{book_id}")
async def delete_book(book_id: int):
    if not db.delete_book(book_id):
        raise HTTPException(status_code=404, detail="Book not found")
    return {"ok": True}


@router.post("/books/{book_id}/read")
async def start_reading(book_id: int):
    _require_book(book_id)
    if not db.add_to_tracker(book_id):
        raise HTTPException(status_code=409, detail="Book is already being tracked")
    return {"ok": True}

@router.get("/tracker")
async def tracker():
    return {"items": db.get_tracked_books()}


@router.put("/tracker/{book_id}/progress")
async def update_progress(book_id: int, data: ProgressIn):
    if not db.update_tracker_progress(book_id, data.current_page):
        raise HTTPException(status_code=404, detail="Book not found in tracker")
    return {"ok": True}


@router.post("/tracker/{book_id}/complete")
async def complete_book(book_id: int, data: CompleteIn):
    if not db.complete_book(book_id, rating=data.rating, review=_blank_to_none(data.review)):
        raise HTTPException(status_code=400, detail="Could not complete book")
    return {"ok": True}


@router.post("/tracker/{book_id}/abandon")
async def abandon_book(book_id: int, data: AbandonIn):
    if not db.abandon_book(book_id, page_at_abandonment=data.page_at_abandonment, reason=_blank_to_none(data.reason)):
        raise HTTPException(status_code=400, detail="Could not abandon book")
    return {"ok": True}


@router.delete("/tracker/{book_id}")
async def remove_from_tracker(book_id: int):
    if not db.remove_from_tracker(book_id):
        raise HTTPException(status_code=404, detail="Book not found in tracker")
    return {"ok": True}


@router.patch("/completed/{book_id}")
async def update_completed(book_id: int, data: CompletedUpdate):
    success = db.update_completed_book(
        book_id=book_id,
        rating=data.rating,
        review=_blank_to_none(data.review),
        start_date=_blank_to_none(data.start_date),
        completion_date=_blank_to_none(data.completion_date)
    )
    if not success:
        raise HTTPException(status_code=404, detail="Completed book not found")
    return {"ok": True}


@router.delete("/completed/{book_id}")
async def remove_from_completed(book_id: int):
    if not db.remove_from_completed(book_id):
        raise HTTPException(status_code=404, detail="Book not found in completed")
    return {"ok": True}


@router.patch("/abandoned/{book_id}")
async def update_abandoned(book_id: int, data: AbandonedUpdate):
    success = db.update_abandoned_book(
        book_id=book_id,
        page_at_abandonment=data.page_at_abandonment,
        reason=_blank_to_none(data.reason),
        start_date=_blank_to_none(data.start_date),
        abandonment_date=_blank_to_none(data.abandonment_date)
    )
    if not success:
        raise HTTPException(status_code=404, detail="Abandoned book not found")
    return {"ok": True}


@router.delete("/abandoned/{book_id}")
async def remove_from_abandoned(book_id: int):
    if not db.remove_from_abandoned(book_id):
        raise HTTPException(status_code=404, detail="Book not found in abandoned")
    return {"ok": True}


@router.post("/sessions", status_code=201)
async def add_session(data: SessionIn):
    _require_book(data.book_id)

    session_id = db.add_reading_session(
        book_id=data.book_id,
        start_time=data.start_time,
        end_time=data.end_time,
        duration_seconds=data.duration_seconds,
        start_page=data.start_page,
        end_page=data.end_page
    )
    db.update_tracker_progress(data.book_id, data.end_page)

    return {"id": session_id}


@router.get("/sessions")
async def list_sessions(page: int = 1, limit: int = 50):
    sessions, total = db.get_all_sessions(limit=limit, offset=(page - 1) * limit)
    return _page(sessions, total, page, limit)


@router.get("/sessions/week")
async def week_sessions(week_start: Optional[str] = None):
    week_start = week_start or _current_week_start()

    try:
        start = datetime.fromisoformat(week_start)
    except ValueError:
        raise HTTPException(status_code=422, detail="week_start must be YYYY-MM-DD")

    prev_week = (start - timedelta(days=7)).date().isoformat()
    next_week = (start + timedelta(days=7)).date().isoformat()

    sessions = db.get_all_weekly_sessions(week_start)
    summary = _week_summary(sessions)
    previous = _week_summary(db.get_all_weekly_sessions(prev_week))

    return {
        "week_start": week_start,
        "prev_week": prev_week,
        "next_week": next_week,
        "sessions": sessions,
        "summary": summary,
        "change": {
            "pages": summary["total_pages"] - previous["total_pages"],
            "days": summary["reading_days"] - previous["reading_days"],
            "pace": round(summary["avg_pages_per_hour"] - previous["avg_pages_per_hour"], 1),
        },
    }


@router.delete("/sessions/{session_id}")
async def delete_session(session_id: int):
    if not db.delete_session(session_id):
        raise HTTPException(status_code=404, detail="Session not found")
    return {"ok": True}


@router.get("/statistics")
async def statistics(year: Optional[int] = None):
    selected_year = datetime.now().year if year is None else year
    stats_year = selected_year if selected_year != 0 else None

    return {
        "selected_year": selected_year,
        "available_years": db.get_available_years(),
        "stats": db.get_year_stats(year=stats_year),
        "monthly_data": db.get_monthly_reading_data(selected_year) if selected_year != 0 else [],
        "top_authors": db.get_top_authors(year=stats_year),
        "top_genres": db.get_top_genres(year=stats_year),
        "rating_distribution": db.get_rating_distribution(year=stats_year),
    }

@router.get("/random/options")
async def random_options():
    return db.get_random_filter_options()


@router.post("/random")
async def random_pick(data: RandomIn):
    book = db.pick_random_book(genre=_blank_to_none(data.genre), author=_blank_to_none(data.author))
    return {
        "book": book,
        "status": db.get_book_status(book["id"]) if book else None,
    }

@router.get("/series/{series_name}")
async def series_books(series_name: str):
    books = db.get_books_by_series(series_name)
    books.sort(key=lambda b: b.get('series_number') or 0)
    return {"items": books}


@router.get("/authors/{author_name}")
async def author_books(author_name: str):
    return {"items": db.get_books_by_author(author_name)}


@router.get("/genres/{genre_name}")
async def genre_books(genre_name: str):
    return {"items": db.get_books_by_genre(genre_name)}

@router.get("/tbr")
async def tbr_lists():
    return {"items": db.get_all_tbr_lists()}


@router.post("/tbr", status_code=201)
async def create_tbr_list(data: TbrListIn):
    return {"id": db.create_tbr_list(name=data.name, description=_blank_to_none(data.description))}


@router.get("/tbr/{list_id}")
async def tbr_list(list_id: int):
    tbr = db.get_tbr_list_by_id(list_id)
    if not tbr:
        raise HTTPException(status_code=404, detail="TBR list not found")
    return {"list": tbr, "items": db.get_books_in_tbr_list(list_id)}


@router.patch("/tbr/{list_id}")
async def update_tbr_list(list_id: int, data: TbrListUpdate):
    if not db.update_tbr_list(list_id=list_id, name=data.name, description=data.description):
        raise HTTPException(status_code=404, detail="TBR list not found")
    return {"ok": True}


@router.delete("/tbr/{list_id}")
async def delete_tbr_list(list_id: int):
    if not db.delete_tbr_list(list_id):
        raise HTTPException(status_code=404, detail="TBR list not found")
    return {"ok": True}


@router.post("/tbr/{list_id}/books")
async def add_to_tbr_list(list_id: int, data: TbrBookIn):
    if not db.add_book_to_tbr_list(book_id=data.book_id, list_id=list_id):
        raise HTTPException(status_code=400, detail="Failed to add book to list")
    return {"ok": True}


@router.delete("/tbr/{list_id}/books/{book_id}")
async def remove_from_tbr_list(list_id: int, book_id: int):
    if not db.remove_book_from_tbr_list(book_id=book_id, list_id=list_id):
        raise HTTPException(status_code=404, detail="Book not found in list")
    return {"ok": True}


@router.post("/tbr/{list_id}/books/{book_id}/up")
async def move_up(list_id: int, book_id: int):
    if not db.move_book_up(book_id=book_id, list_id=list_id):
        raise HTTPException(status_code=400, detail="Cannot move book up")
    return {"ok": True}


@router.post("/tbr/{list_id}/books/{book_id}/down")
async def move_down(list_id: int, book_id: int):
    if not db.move_book_down(book_id=book_id, list_id=list_id):
        raise HTTPException(status_code=400, detail="Cannot move book down")
    return {"ok": True}

def _with_existing(book: Dict) -> Dict:
    existing = db.get_book_by_isbn(book["isbn"]) if book.get("isbn") else None
    return {**book, "existing_book_id": existing["id"] if existing else None}


@router.get("/lookup/isbn/{isbn}")
def lookup_isbn(isbn: str):
    try:
        book = goodreads.lookup_isbn(isbn)
    except ValueError:
        raise HTTPException(status_code=422, detail="Not a valid ISBN")
    except goodreads.GoodreadsError as e:
        raise HTTPException(status_code=502, detail=str(e))

    if not book:
        raise HTTPException(status_code=404, detail="Goodreads has no book with that ISBN")
    return _with_existing(book)


@router.get("/lookup/search")
def lookup_search(q: str):
    try:
        return {"items": goodreads.search(q)}
    except goodreads.GoodreadsError as e:
        raise HTTPException(status_code=502, detail=str(e))


@router.get("/lookup/book")
def lookup_book(url: str):
    try:
        book = goodreads.lookup_url(url)
    except ValueError:
        raise HTTPException(status_code=422, detail="Not a Goodreads book URL")
    except goodreads.GoodreadsError as e:
        raise HTTPException(status_code=502, detail=str(e))

    if not book:
        raise HTTPException(status_code=404, detail="Could not read that Goodreads page")
    return _with_existing(book)
