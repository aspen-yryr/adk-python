import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import func
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import DateTime, String

from .base import Base
from .constants import DEFAULT_MAX_KEY_LENGTH
from .types import DynamicJSON

if TYPE_CHECKING:
    from .event import StorageEvent


class StorageSession(Base):
    """Represents a session stored in the database."""

    __tablename__ = "sessions"

    app_name: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
    )

    state: Mapped[MutableDict[str, Any]] = mapped_column(
        MutableDict.as_mutable(DynamicJSON), default={}
    )

    create_time: Mapped[datetime] = mapped_column(DateTime(), default=func.now())
    update_time: Mapped[datetime] = mapped_column(
        DateTime(), default=func.now(), onupdate=func.now()
    )

    storage_events: Mapped[list["StorageEvent"]] = relationship(
        "StorageEvent",
        back_populates="storage_session",
    )

    def __repr__(self):
        return f"<StorageSession(id={self.id}, update_time={self.update_time})>"
