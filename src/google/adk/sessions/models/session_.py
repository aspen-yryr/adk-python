from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Optional
from typing import TYPE_CHECKING
import uuid

from sqlalchemy import func
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.types import DateTime
from sqlalchemy.types import String

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

  @property
  def _dialect_name(self) -> Optional[str]:
    session = inspect(self).session
    if session is None:
      return None
    assert session.bind is not None, "Session must have a bound engine."

    return session.bind.dialect.name

  @property
  def update_timestamp_tz(self) -> float:
    """Returns the time zone aware update timestamp."""
    if self._dialect_name == "sqlite":
      # SQLite does not support timezone. SQLAlchemy returns a naive datetime
      # object without timezone information. We need to convert it to UTC
      # manually.
      return self.update_time.replace(tzinfo=timezone.utc).timestamp()
    return self.update_time.timestamp()
