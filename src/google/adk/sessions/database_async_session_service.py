# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import annotations

import copy
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator, Optional

from sqlalchemy import delete, select
from sqlalchemy.exc import ArgumentError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.schema import MetaData
from typing_extensions import override
from tzlocal import get_localzone

from ..events.event import Event
from .base_session_service import (
    BaseSessionService,
    GetSessionConfig,
    ListSessionsResponse,
)
from .models import (
    Base,
    StorageAppState,
    StorageEvent,
    StorageSession,
    StorageUserState,
)
from .session import Session
from .state import State

logger = logging.getLogger("google_adk." + __name__)


class DatabaseAsyncSessionService(BaseSessionService):
    """A session service that uses a database for storage."""

    def __init__(self, db_url: str, **kwargs: Any):
        """Initializes the database session service with a database URL."""
        # 1. Create DB engine for db connection
        self.db_url = db_url

        try:
            db_engine = create_async_engine(db_url, **kwargs)
        except Exception as e:
            if isinstance(e, ArgumentError):
                raise ValueError(
                    f"Invalid database URL format or argument '{db_url}'."
                ) from e
            if isinstance(e, ImportError):
                raise ValueError(
                    f"Database related module not found for URL '{db_url}'.\nCheck if the required database driver is installed."
                ) from e
            raise ValueError(
                f"Failed to create database engine for URL '{db_url}'"
            ) from e

        # Get the local timezone
        local_timezone = get_localzone()
        logger.info(f"Local timezone: {local_timezone}")

        self.db_engine: AsyncEngine = db_engine
        self.metadata: MetaData = MetaData()

        # DB session factory method
        self.database_session_factory: async_sessionmaker[AsyncSession] = (
            async_sessionmaker(bind=self.db_engine)
        )

    @asynccontextmanager
    async def database_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Context manager for database session."""
        async with async_sessionmaker(
            bind=create_async_engine(self.db_url)
        )() as session:
            try:
                yield session
            except Exception as e:
                await session.rollback()
                raise e
            finally:
                await session.close()

    async def create_tables(self) -> None:
        """Creates all tables in the database."""
        async with self.db_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
            await conn.commit()
            await conn.close()

    @override
    async def create_session(
        self,
        *,
        app_name: str,
        user_id: str,
        state: Optional[dict[str, Any]] = None,
        session_id: Optional[str] = None,
    ) -> Session:
        # 1. Populate states.
        # 2. Build storage session object
        # 3. Add the object to the table
        # 4. Build the session object with generated id
        # 5. Return the session

        async with self.database_session() as db_session:
            # Fetch app and user states from storage
            storage_app_state = await db_session.get(StorageAppState, (app_name))
            storage_user_state = await db_session.get(
                StorageUserState, (app_name, user_id)
            )

            app_state = (
                storage_app_state.state if storage_app_state else MutableDict({})
            )
            user_state = (
                storage_user_state.state if storage_user_state else MutableDict({})
            )

            # Create state tables if not exist
            if not storage_app_state:
                storage_app_state = StorageAppState(app_name=app_name, state={})
                db_session.add(storage_app_state)
            if not storage_user_state:
                storage_user_state = StorageUserState(
                    app_name=app_name, user_id=user_id, state={}
                )
                db_session.add(storage_user_state)

            # Extract state deltas
            app_state_delta, user_state_delta, session_state = _extract_state_delta(
                state or {}
            )

            # Apply state delta
            app_state.update(app_state_delta)
            user_state.update(user_state_delta)

            # Store app and user state
            if app_state_delta:
                storage_app_state.state = app_state
            if user_state_delta:
                storage_user_state.state = user_state

            # Store the session
            storage_session = StorageSession(
                app_name=app_name,
                user_id=user_id,
                id=session_id,
                state=session_state,
            )

            db_session.add(storage_session)

            await db_session.commit()
            await db_session.refresh(storage_session)

            # Merge states for response
            merged_state = _merge_state(app_state, user_state, session_state)
            session = Session(
                app_name=str(storage_session.app_name),
                user_id=str(storage_session.user_id),
                id=str(storage_session.id),
                state=merged_state,
                last_update_time=storage_session.update_time.timestamp(),
            )
            return session

    @override
    async def get_session(
        self,
        *,
        app_name: str,
        user_id: str,
        session_id: str,
        config: Optional[GetSessionConfig] = None,
    ) -> Optional[Session]:
        # 1. Get the storage session entry from session table
        # 2. Get all the events based on session id and filtering config
        # 3. Convert and return the session
        async with self.database_session() as db_session:
            storage_session = await db_session.get(
                StorageSession, (app_name, user_id, session_id)
            )
            if storage_session is None:
                return None

            if config and config.after_timestamp:
                after_dt = datetime.fromtimestamp(config.after_timestamp)
                timestamp_filter = StorageEvent.timestamp >= after_dt
            else:
                timestamp_filter = StorageEvent.id == StorageEvent.id  # No filter

            stmt = (
                select(StorageEvent)
                .where(
                    StorageEvent.session_id == storage_session.id,
                    timestamp_filter,
                )
                .order_by(StorageEvent.timestamp.desc())
                .limit(
                    config.num_recent_events
                    if config and config.num_recent_events
                    else None
                )
            )
            storage_events = (await db_session.execute(stmt)).scalars().all()

            # Fetch states from storage
            storage_app_state = await db_session.get(StorageAppState, (app_name))
            storage_user_state = await db_session.get(
                StorageUserState, (app_name, user_id)
            )

            app_state = storage_app_state.state if storage_app_state else {}
            user_state = storage_user_state.state if storage_user_state else {}
            session_state = storage_session.state

            # Merge states
            merged_state = _merge_state(app_state, user_state, session_state)

            # Convert storage session to session
            session = Session(
                app_name=app_name,
                user_id=user_id,
                id=session_id,
                state=merged_state,
                last_update_time=storage_session.update_time.timestamp(),
            )
            session.events = [e.to_event() for e in reversed(storage_events)]
        return session

    @override
    async def list_sessions(
        self, *, app_name: str, user_id: str
    ) -> ListSessionsResponse:
        async with self.database_session() as db_session:
            stmt = (
                select(StorageSession)
                .where(
                    StorageSession.app_name == app_name,
                    StorageSession.user_id == user_id,
                )
                .order_by(StorageSession.update_time.desc())
            )
            results = (await db_session.execute(stmt)).scalars().all()
            sessions = []
            for storage_session in results:
                session = Session(
                    app_name=app_name,
                    user_id=user_id,
                    id=storage_session.id,
                    state={},
                    last_update_time=storage_session.update_time.timestamp(),
                )
                sessions.append(session)
            return ListSessionsResponse(sessions=sessions)

    @override
    async def delete_session(
        self, *, app_name: str, user_id: str, session_id: str
    ) -> None:
        async with self.database_session() as db_session:
            stmt = delete(StorageSession).where(
                StorageSession.app_name == app_name,
                StorageSession.user_id == user_id,
                StorageSession.id == session_id,
            )
            await db_session.execute(stmt)
            await db_session.commit()

    @override
    async def append_event(self, session: Session, event: Event) -> Event:
        logger.info(f"Append event: {event} to session {session.id}")

        if event.partial:
            return event

        # 1. Check if timestamp is stale
        # 2. Update session attributes based on event config
        # 3. Store event to table
        async with self.database_session() as db_session:
            storage_session = await db_session.get(
                StorageSession, (session.app_name, session.user_id, session.id)
            )
            assert storage_session is not None, (
                f"Session {session.id} not found in storage for app {session.app_name} and user {session.user_id}."
            )

            if storage_session.update_time.timestamp() > session.last_update_time:
                raise ValueError(
                    "The last_update_time provided in the session object"
                    f" {datetime.fromtimestamp(session.last_update_time):'%Y-%m-%d %H:%M:%S'} is"
                    " earlier than the update_time in the storage_session"
                    f" {storage_session.update_time:'%Y-%m-%d %H:%M:%S'}. Please check"
                    " if it is a stale session."
                )

            # Fetch states from storage
            storage_app_state = await db_session.get(
                StorageAppState, (session.app_name)
            )
            storage_user_state = await db_session.get(
                StorageUserState, (session.app_name, session.user_id)
            )
            if storage_app_state is None or storage_user_state is None:
                raise ValueError(
                    f"App state or user state not found for app {session.app_name} and user {session.user_id}."
                )

            app_state = storage_app_state.state
            user_state = storage_user_state.state
            session_state = storage_session.state

            # Extract state delta
            app_state_delta = {}
            user_state_delta = {}
            session_state_delta = {}
            if event.actions:
                if event.actions.state_delta:
                    app_state_delta, user_state_delta, session_state_delta = (
                        _extract_state_delta(event.actions.state_delta)
                    )

            # Merge state and update storage
            if app_state_delta:
                app_state.update(app_state_delta)
                storage_app_state.state = app_state
            if user_state_delta:
                user_state.update(user_state_delta)
                storage_user_state.state = user_state
            if session_state_delta:
                session_state.update(session_state_delta)
                storage_session.state = session_state

            db_session.add(StorageEvent.from_event(session, event))

            await db_session.commit()
            await db_session.refresh(storage_session)

            # Update timestamp with commit time
            session.last_update_time = storage_session.update_time.timestamp()

        # Also update the in-memory session
        await super().append_event(session=session, event=event)
        return event


def _extract_state_delta(state: dict[str, Any]):
    app_state_delta = {}
    user_state_delta = {}
    session_state_delta = {}
    if state:
        for key in state.keys():
            if key.startswith(State.APP_PREFIX):
                app_state_delta[key.removeprefix(State.APP_PREFIX)] = state[key]
            elif key.startswith(State.USER_PREFIX):
                user_state_delta[key.removeprefix(State.USER_PREFIX)] = state[key]
            elif not key.startswith(State.TEMP_PREFIX):
                session_state_delta[key] = state[key]
    return app_state_delta, user_state_delta, session_state_delta


def _merge_state(app_state, user_state, session_state):
    # Merge states for response
    merged_state = copy.deepcopy(session_state)
    for key in app_state.keys():
        merged_state[State.APP_PREFIX + key] = app_state[key]
    for key in user_state.keys():
        merged_state[State.USER_PREFIX + key] = user_state[key]
    return merged_state
