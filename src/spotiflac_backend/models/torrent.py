from dataclasses import dataclass

@dataclass
class TorrentInfo:
    title: str       # название раздачи
    url: str         # ссылка на .torrent или magnet
    size: str        # например "55.2 MB"
    seeders: int     # сидеров
    leechers: int    # личеров