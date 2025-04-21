from tap_youtube_analytics.streams.channels import Channels
from tap_youtube_analytics.streams.playlists import Playlists
from tap_youtube_analytics.streams.playlist_items import PlaylistItems
from tap_youtube_analytics.streams.videos import Videos

STREAMS = {
    "channels": Channels,
    "playlists": Playlists,
    "playlist_items": PlaylistItems,
    "videos": Videos,
}
