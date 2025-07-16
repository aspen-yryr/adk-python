import json

from sqlalchemy import Dialect
from sqlalchemy import Text
from sqlalchemy.dialects import mysql
from sqlalchemy.dialects import postgresql
from sqlalchemy.types import DateTime
from sqlalchemy.types import TypeDecorator


class DynamicJSON(TypeDecorator):
  """A JSON-like type that uses JSONB on PostgreSQL and TEXT with JSON serialization for other databases."""

  impl = Text  # Default implementation is TEXT

  def load_dialect_impl(self, dialect: Dialect):
    if dialect.name == "postgresql":
      return dialect.type_descriptor(postgresql.JSONB())
    if dialect.name == "mysql":
      # Use LONGTEXT for MySQL to address the data too long issue
      return dialect.type_descriptor(mysql.LONGTEXT())
    return dialect.type_descriptor(Text())  # Default to Text for other dialects

  def process_bind_param(self, value, dialect: Dialect):
    if value is not None:
      if dialect.name == "postgresql":
        return value  # JSONB handles dict directly
      return json.dumps(value)  # Serialize to JSON string for TEXT
    return value

  def process_result_value(self, value, dialect: Dialect):
    if value is not None:
      if dialect.name == "postgresql":
        return value  # JSONB returns dict directly
      else:
        return json.loads(value)  # Deserialize from JSON string for TEXT
    return value


class PreciseTimestamp(TypeDecorator):
  """Represents a timestamp precise to the microsecond."""

  impl = DateTime
  cache_ok = True

  def load_dialect_impl(self, dialect: Dialect):
    if dialect.name == "mysql":
      return dialect.type_descriptor(mysql.DATETIME(timezone=False, fsp=6))
    return dialect.type_descriptor(DateTime(timezone=False))
