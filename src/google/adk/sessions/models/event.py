import json
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import Boolean, ForeignKeyConstraint, Text, func
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.types import PickleType, String

from ...events import Event
from .. import _session_util
from ..session import Session
from .base import Base
from .constants import DEFAULT_MAX_KEY_LENGTH, DEFAULT_MAX_VARCHAR_LENGTH
from .types import DynamicJSON, PreciseTimestamp

if TYPE_CHECKING:
    from .session_ import StorageSession


class StorageEvent(Base):
    """Represents an event stored in the database."""

    __tablename__ = "events"

    id: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), primary_key=True)
    app_name: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    user_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )
    session_id: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
    )

    invocation_id: Mapped[str] = mapped_column(String(DEFAULT_MAX_VARCHAR_LENGTH))
    author: Mapped[str] = mapped_column(String(DEFAULT_MAX_VARCHAR_LENGTH))
    branch: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), nullable=True
    )
    timestamp: Mapped[PreciseTimestamp] = mapped_column(
        PreciseTimestamp, default=func.now()
    )
    content: Mapped[dict[str, Any]] = mapped_column(DynamicJSON, nullable=True)
    actions: Mapped[MutableDict[str, Any]] = mapped_column(PickleType)

    long_running_tool_ids_json: Mapped[Optional[str]] = mapped_column(
        Text, nullable=True
    )
    grounding_metadata: Mapped[dict[str, Any]] = mapped_column(
        DynamicJSON, nullable=True
    )
    partial: Mapped[bool] = mapped_column(Boolean, nullable=True)
    turn_complete: Mapped[bool] = mapped_column(Boolean, nullable=True)
    error_code: Mapped[str] = mapped_column(
        String(DEFAULT_MAX_VARCHAR_LENGTH), nullable=True
    )
    error_message: Mapped[str] = mapped_column(String(1024), nullable=True)
    interrupted: Mapped[bool] = mapped_column(Boolean, nullable=True)

    storage_session: Mapped["StorageSession"] = relationship(
        "StorageSession",
        back_populates="storage_events",
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["app_name", "user_id", "session_id"],
            ["sessions.app_name", "sessions.user_id", "sessions.id"],
            ondelete="CASCADE",
        ),
    )

    @property
    def long_running_tool_ids(self) -> set[str]:
        return (
            set(json.loads(self.long_running_tool_ids_json))
            if self.long_running_tool_ids_json
            else set()
        )

    @long_running_tool_ids.setter
    def long_running_tool_ids(self, value: set[str]):
        if value is None:
            self.long_running_tool_ids_json = None
        else:
            self.long_running_tool_ids_json = json.dumps(list(value))

    @classmethod
    def from_event(cls, session: Session, event: Event) -> "StorageEvent":
        storage_event = StorageEvent(
            id=event.id,
            invocation_id=event.invocation_id,
            author=event.author,
            branch=event.branch,
            actions=event.actions,
            session_id=session.id,
            app_name=session.app_name,
            user_id=session.user_id,
            timestamp=datetime.fromtimestamp(event.timestamp),
            long_running_tool_ids=event.long_running_tool_ids,
            partial=event.partial,
            turn_complete=event.turn_complete,
            error_code=event.error_code,
            error_message=event.error_message,
            interrupted=event.interrupted,
        )
        if event.content:
            storage_event.content = event.content.model_dump(
                exclude_none=True, mode="json"
            )
        if event.grounding_metadata:
            storage_event.grounding_metadata = event.grounding_metadata.model_dump(
                exclude_none=True, mode="json"
            )
        return storage_event

    def to_event(self) -> Event:
        return Event(
            id=self.id,
            invocation_id=self.invocation_id,
            author=self.author,
            branch=self.branch,
            actions=self.actions,
            timestamp=self.timestamp.timestamp(),
            content=_session_util.decode_content(self.content),
            long_running_tool_ids=self.long_running_tool_ids,
            partial=self.partial,
            turn_complete=self.turn_complete,
            error_code=self.error_code,
            error_message=self.error_message,
            interrupted=self.interrupted,
            grounding_metadata=_session_util.decode_grounding_metadata(
                self.grounding_metadata
            ),
        )
