import pytest
import respx
from httpx import Response
from spotiflac_backend.services.rutracker import RutrackerService
from spotiflac_backend.models.torrent import TorrentInfo

SAMPLE_HTML = """
<table>
  <tr class="forumHeaderBorder"><th>Header</th></tr>
  <tr>
    <td><a href="/forum/viewtopic.php?t=1">Test Album FLAC</a></td>
    <td>uploader</td>
    <td></td><td>55.2 MB</td>
    <td>123/45</td>
  </tr>
</table>
"""

@pytest.mark.asyncio
@respx.mock
async def test_search_parses_correctly():
    base = "https://rutracker.example"
    route = respx.get(f"{base}/forum/tracker.php").respond(200, text=SAMPLE_HTML)

    svc = RutrackerService(base_url=base)
    result = await svc.search("Test")
    await svc.close()

    assert route.called
    assert len(result) == 1

    info = result[0]
    assert isinstance(info, TorrentInfo)
    assert info.title == "Test Album FLAC"
    assert info.url == f"{base}/forum/viewtopic.php?t=1"
    assert info.size == "55.2 MB"
    assert info.seeders == 123
    assert info.leechers == 45
