"""Small FastAPI service for ROE admin workflows."""

from __future__ import annotations

import os
from typing import Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from pydantic import BaseModel, Field

from db import get_db
from roe_service import create_violation, fetch_player_candidates, get_summary, list_violations

load_dotenv()

app = FastAPI(title="STFC ROE API", version="1.0.0")


def _admin_password() -> str:
    """Return the shared admin password used for the ROE tools."""
    return os.getenv("NCC_ADMIN_PASSWORD") or os.getenv("ADMIN_PASSWORD") or "salsa"


def require_admin(x_admin_password: Optional[str] = Header(default=None)) -> None:
    """Guard write-capable ROE admin endpoints."""
    if x_admin_password != _admin_password():
        raise HTTPException(status_code=401, detail="Invalid admin password")


class OffenderOverrides(BaseModel):
    """Optional manual overrides when the offender cannot be resolved cleanly."""

    name: str = ""
    alliance_id: str = ""
    alliance_tag: str = ""
    alliance_name: str = ""


class CreateViolationRequest(BaseModel):
    """Payload for creating a violation."""

    offender_query: str = Field(min_length=1)
    violation_type: str = Field(min_length=1)
    reported_by: str = ""
    victim_name: str = ""
    victim_player_id: str = ""
    system_name: str = ""
    notes: str = ""
    offense_date: str = ""
    source: str = "manual-ui"
    source_ref: str = ""
    offender_overrides: OffenderOverrides = Field(default_factory=OffenderOverrides)


@app.get("/api/healthz")
def healthcheck():
    """Basic uptime probe."""
    return {"status": "ok"}


@app.get("/api/players/search")
def search_players(
    q: str = Query(default="", min_length=1),
    limit: int = Query(default=8, ge=1, le=25),
    _auth: None = Depends(require_admin),
):
    """Search players for the ROE entry UI."""
    conn = get_db()
    try:
        return {"players": fetch_player_candidates(conn, q, limit)}
    finally:
        conn.close()


@app.get("/api/roe/summary")
def roe_summary(_auth: None = Depends(require_admin)):
    """Return summary stats, tallies, and recent incidents."""
    conn = get_db()
    try:
        return get_summary(conn)
    finally:
        conn.close()


@app.get("/api/roe/violations")
def roe_violations(
    limit: int = Query(default=50, ge=1, le=200),
    _auth: None = Depends(require_admin),
):
    """Return recent violations."""
    conn = get_db()
    try:
        return {"violations": list_violations(conn, limit)}
    finally:
        conn.close()


@app.post("/api/roe/violations")
def create_roe_violation(payload: CreateViolationRequest, _auth: None = Depends(require_admin)):
    """Create a violation and return the new record context."""
    conn = get_db()
    try:
        try:
            result = create_violation(
                conn,
                offender_query=payload.offender_query,
                violation_type=payload.violation_type,
                reported_by=payload.reported_by,
                victim_name=payload.victim_name,
                victim_player_id=payload.victim_player_id,
                system_name=payload.system_name,
                notes=payload.notes,
                offense_date=payload.offense_date,
                source=payload.source,
                source_ref=payload.source_ref,
                offender_overrides=payload.offender_overrides.model_dump(),
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return result
    finally:
        conn.close()
