# Federation events

## live.upcoming.detected
Observed a public upcoming livestream.

## live.started
Observed a public livestream whose `actualStartTime` exists.

## live.ended
Observed a public livestream whose `actualEndTime` exists.

All three use this envelope:

```json
{
  "kind": "event",
  "name": "live.started",
  "source": "live-watcher",
  "payload": {
    "channel_id": "UC...",
    "video_id": "...",
    "title": "...",
    "youtube_url": "https://www.youtube.com/watch?v=...",
    "privacy_status": "public",
    "scheduled_start_time": null,
    "actual_start_time": null,
    "actual_end_time": null,
    "detected_via": "websub"
  },
  "metadata": {
    "watcher_version": "0.1.0",
    "watcher_instance": "default"
  }
}
```

The SQLite emitted flag is set only after Central Node returns a successful HTTP response.
