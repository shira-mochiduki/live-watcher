# Live Watcher architecture

Live Watcher is an independent project in the federation.

## Owns
- YouTube WebSub subscription/callbacks
- RSS safety monitoring
- `/live` probe monitoring
- `videos.list` confirmation and batching
- `privacyStatus=public` filtering
- upcoming/live/ended observation
- scheduled-start-aware polling and backoff
- local SQLite observation state
- publishing observed facts to Central Node

## Does not own
- Discord notifications
- STREAM COCKPIT decisions
- Stream Commander policy
- Stream Hangar data
- Central Node routing
- VCal audio processing

## Relationship

```text
YouTube
  ↓
Live Watcher
  │ event
  ▼
Central Node
  ├─ Stream Commander
  ├─ STREAM COCKPIT
  └─ future projects
```

Central Node is used for events/commands. It is not a mandatory proxy for every direct API request between projects.
