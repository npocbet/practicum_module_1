import datetime
import uuid
from dataclasses import dataclass, field


@dataclass(frozen=True)
class GenreDc:
    name: str
    description: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class PersonDc:
    full_name: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class FilmWorkDc:
    title: str
    description: str
    creation_date: datetime.date
    rating: float = field(default=0.0)
    type: str = field(default='movie')
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class GenreFilmworkDc:
    film_work: uuid.UUID
    genre: uuid.UUID
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class PersonFilmworkDc:
    film_work: uuid.UUID
    person: uuid.UUID
    role: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
