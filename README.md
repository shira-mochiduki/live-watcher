# Live Watcher

Live Watcher is the independent YouTube monitoring project in the federation.

It is based on the proven monitoring logic from the existing
`youtube-live-websub-notifier`, while intentionally removing Discord notification
responsibility.

## v0.1.0

Preserved:
- WebSub
- RSS SAFETY
- `/live` probe
- `videos.list` batching
- public-only filtering
- upcoming monitoring
- 10-minute pre-start monitoring window
- post-schedule backoff
- SQLite / Render Persistent Disk support

Changed:
- Discord webhook notification removed
- `CHANNELS_JSON` now needs only `channel_id`
- observed states are published to Central Node
- notification flags became federation event-emission flags

## Federation flow

```text
YouTube
   ↓
Live Watcher
   ↓
Central Node
   ↓
independent subscriber projects
```

Live Watcher publishes:
- `live.upcoming.detected`
- `live.started`
- `live.ended`

For the first integration test, Central Node only needs to route `live.started`.

## CHANNELS_JSON

```json
[{"channel_id":"UCxxxxxxxxxxxxxxxxxxxxxx"}]
```

No Discord webhook or mention is required.

## Central Node

Set:

```text
CENTRAL_NODE_URL=https://your-central-node.onrender.com
```

Live Watcher posts to:

```text
<CENTRAL_NODE_URL>/messages
```

If Central Node ingress authentication is enabled, set `CENTRAL_NODE_TOKEN` to the
same value as Central Node's `CENTRAL_NODE_INGRESS_TOKEN`.

## Render

Build:

```text
pip install -r requirements.txt
```

Start:

```text
uvicorn main:app --host 0.0.0.0 --port $PORT
```

Mount a Persistent Disk at `/var/data` if state should survive redeploys.

## Safe migration

Keep `youtube-live-websub-notifier` running while testing this repository.

Recommended order:

```text
1. Deploy live-watcher separately
2. Confirm /health
3. Confirm WebSub VERIFY
4. Confirm RSS SAFETY and /live probe
5. Configure CENTRAL_NODE_URL
6. Confirm live.started reaches Central Node
7. Connect Stream Commander
8. Retire the old notifier only after stable operation
```
