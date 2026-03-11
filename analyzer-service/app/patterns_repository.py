from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict

from sqlalchemy import DateTime, Float, String, Text, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    pass


class TechPatternRow(Base):
    __tablename__ = "tech_patterns"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(128), nullable=False, default="other")
    patterns_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    aliases_json: Mapped[str] = mapped_column(Text, nullable=False, default="[]")
    weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class PatternsRepository:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, future=True)
        self.session_factory = sessionmaker(self.engine, expire_on_commit=False)

    def ensure_schema(self) -> None:
        Base.metadata.create_all(self.engine)

    def load_all(self) -> Dict[str, Dict[str, Any]]:
        self.ensure_schema()
        with self.session_factory() as session:
            rows = session.scalars(select(TechPatternRow)).all()

        patterns: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            patterns[row.id] = {
                "name": row.name,
                "category": row.category,
                "patterns": json.loads(row.patterns_json or "[]"),
                "weight": float(row.weight),
                "aliases": json.loads(row.aliases_json or "[]"),
                "description": row.description or "",
            }
        return patterns

    def save_all(self, patterns: Dict[str, Dict[str, Any]]) -> None:
        self.ensure_schema()
        now = datetime.now(timezone.utc)
        incoming_ids = {str(key) for key in patterns.keys()}

        with self.session_factory() as session:
            existing = {row.id: row for row in session.scalars(select(TechPatternRow)).all()}

            for tech_id, payload in patterns.items():
                tech_key = str(tech_id)
                row = existing.get(tech_key)
                if row is None:
                    row = TechPatternRow(
                        id=tech_key,
                        name=str(payload.get("name", tech_key)),
                        category=str(payload.get("category", "other")),
                        patterns_json="[]",
                        aliases_json="[]",
                        weight=float(payload.get("weight", 1.0) or 1.0),
                        description=str(payload.get("description", "")),
                        created_at=now,
                        updated_at=now,
                    )
                    session.add(row)

                row.name = str(payload.get("name", tech_key))
                row.category = str(payload.get("category", "other"))
                row.patterns_json = json.dumps(payload.get("patterns", []), ensure_ascii=False)
                row.aliases_json = json.dumps(payload.get("aliases", []), ensure_ascii=False)
                row.weight = float(payload.get("weight", 1.0) or 1.0)
                row.description = str(payload.get("description", ""))
                row.updated_at = now

            stale_rows = [row for key, row in existing.items() if key not in incoming_ids]
            for row in stale_rows:
                session.delete(row)

            session.commit()
