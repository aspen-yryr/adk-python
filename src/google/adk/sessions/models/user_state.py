from typing import Any

from sqlalchemy import func
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import DateTime
from sqlalchemy.types import String

from .base import Base
from .constants import DEFAULT_MAX_KEY_LENGTH
from .types import DynamicJSON


class StorageUserState(Base):
  """Represents a user state stored in the database."""

  __tablename__ = "user_states"

  app_name: Mapped[str] = mapped_column(
      String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
  )
  user_id: Mapped[str] = mapped_column(
      String(DEFAULT_MAX_KEY_LENGTH), primary_key=True
  )
  state: Mapped[MutableDict[str, Any]] = mapped_column(
      MutableDict.as_mutable(DynamicJSON), default={}
  )
  update_time: Mapped[DateTime] = mapped_column(
      DateTime(), default=func.now(), onupdate=func.now()
  )
