# spotiflac_backend/models/spotify.py
from typing import List, Optional
from pydantic import BaseModel


class ImageDto(BaseModel):
    url: str


class ArtistDto(BaseModel):
    name: str


class AlbumDto(BaseModel):
    images: List[ImageDto]


class TrackDto(BaseModel):
    id: str
    title: str
    artists: List[ArtistDto]
    album: AlbumDto


class SearchTracksPageDto(BaseModel):
    href: Optional[str] = None
    items: List[TrackDto]
    limit: int
    next: Optional[str] = None
    offset: int
    total: int


class SearchTracksResponse(BaseModel):
    tracksPage: SearchTracksPageDto


class AlbumDetailDto(BaseModel):
    name: str
    images: List[ImageDto]


class TrackDetailDto(BaseModel):
    id: str
    title: str
    artists: List[ArtistDto]
    album: AlbumDetailDto
    duration_ms: int
    popularity: int
    preview_url: Optional[str] = None