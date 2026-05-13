# Yellowpages Hermes plugin

Connects a Hermes agent to the Yellowpages messaging platform. The adapter
polls Yellowpages for new human-authored messages, dispatches them to the
agent, and posts the agent's replies back.

## Install

Install the yellowpages plugin through hermes.

```bash
hermes plugins install silver-carbon/yellowpages
```

The Hermes plugin loader discovers any directory under `~/.hermes/plugins/`
that contains a `plugin.yaml` manifest. The manifest filename **must** be
lowercase — the loader is case-sensitive on Linux.

## Configure

1. Put your API token in `~/.hermes/.env`:

   ```
   YELLOWPAGES_TOKEN=<bearer-token-issued-by-yellowpages>
   YELLOWPAGES_ALLOW_ALL_USERS=true
   ```

   `YELLOWPAGES_TOKEN` identifies the agent server-side; the adapter sends
   it as `Authorization: Bearer <token>` on every request.

   `YELLOWPAGES_ALLOW_ALL_USERS=true` admits every `humanId` the YP server
   delivers, bypassing Hermes' default DM-pairing flow. The pairing flow
   would otherwise greet each new human with a chat message asking the dev
   to run `hermes pairing approve yellowpages <code>` in a terminal. YP's
   server is the identity provider, so the agent trusts whatever `humanId`
   it sees on `/inbox`. Drop this line if you want the pairing prompt.

2. Enable the platform in `~/.hermes/config.yaml`:

   ```yaml
   gateway:
     platforms:
       yellowpages:
         enabled: true
   ```

## Run

```bash
hermes gateway start
```

On a clean start you should see:

```
INFO Yellowpages: connected, polling https://kpjowqfgbvpmjzgvylbo.supabase.co/functions/v1/api every 5s
```

## How it works

### Inbox poll

Every 5 seconds the adapter calls `GET {API_BASE_URL}/inbox?agentId=1`.
The `agentId` is hardcoded for now (see the TODO in `adapter.py`) and
should be promoted to configuration once multiple agents share the
deployment. The response is a list of `HumanMessage`:

```json
{
  "id": 42,
  "humanId": 7,
  "conversationId": 11,
  "body": "hello",
  "createdAt": 1715520000000,
  "seenByAgent": false
}
```

Each message is dispatched as a separate `MessageEvent` (one task per
message, so a slow handler does not delay the next).

**Deduplication is the server's job.** The adapter does not maintain a
client-side seen-set — it trusts `/inbox` to honour `seenByAgent` and stop
returning messages once they've been processed.

### Send

When the agent produces a reply, the gateway calls
`send(chat_id, content, reply_to=...)`. The adapter posts to
`{API_BASE_URL}/message` with:

```json
{ "humanId": <int>, "body": <string>, "replyId": <int> }
```

- `chat_id` (= `conversationId`) is mapped to `humanId` using the cache
  built from the most recent `/inbox` responses.
- `replyId` is the id of the human message being responded to. The API
  requires every agent message to reference one. The adapter uses the
  gateway's `reply_to` when present; for autonomous sends (no
  `reply_to`) it falls back to the most recent human message cached for
  that conversation.

The API is "currently open" — no JWT is required — so sender identity is
not derived from the bearer token. The token is still sent on every
request for forward compatibility.

### Mapping notes

- `conversationId` → Hermes `chat_id` / `chat_name`
- `humanId` → Hermes `user_id` / `user_name`
- `chat_type` is always `"dm"` (each human has their own conversation)

## Definition of done

- [x] All four files present in `yellowpages/`
- [x] Lowercase `plugin.yaml` filename (loader is case-sensitive on Linux)
- [x] Plugin loads without exceptions on Hermes startup
- [x] Two inbox items → two `MessageEvent`s dispatched with distinct `chat_id`s
- [x] Each agent reply produces one `POST /messages` call

## Troubleshooting

| Symptom | Likely cause |
| --- | --- |
| `RuntimeError: YELLOWPAGES_TOKEN env var is required` at startup | Token missing from `~/.hermes/.env`. |
| `Platform 'yellowpages' requirements not met` in the log | `check_fn` saw no token. Same fix as above. |
| Poll loop logs `poll failed: 401` repeatedly | Token rejected by the server. Rotate / re-issue. |
| `send()` returns `Unknown conversationId ... — no humanId cached from /inbox` | Send was attempted for a conversation the adapter never polled. Usually a misrouted reply; restart the gateway so the next poll repopulates the cache. |
| `send()` returns `No replyId available for conversationId ...` | An autonomous send was attempted for a conversation with no cached human messages. The API requires every agent message to carry a `replyId`. |
| Agent receives the same message every 5 seconds | Server is not flipping `seenByAgent` when `/inbox` is consumed. The adapter trusts server-side dedup. |

## Configuration knobs

| Env var | Effect |
| --- | --- |
| `YELLOWPAGES_TOKEN` | Required. Agent bearer token. |
| `YELLOWPAGES_ALLOW_ALL_USERS=true` | Bypass Hermes DM pairing — every `humanId` the server delivers is authorized. Without it, the first message from each `humanId` triggers a pairing-code prompt the dev must approve via `hermes pairing approve`. |
| `YELLOWPAGES_HOME_CHANNEL` | Cron delivery target chat_id for `deliver=yellowpages`. The adapter defaults this to the sentinel `disabled` in `connect()` so the "📬 No home channel is set" prompt never reaches end users; override with a real conversationId if you wire up cron-to-YP. |

`API_BASE_URL` and `POLL_INTERVAL_SECONDS` are hardcoded at the top of
`adapter.py`; change them there if needed. If multiple environments
(staging / prod) become a requirement, promote `API_BASE_URL` to an env
var.
