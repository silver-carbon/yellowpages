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
   ```

   `YELLOWPAGES_TOKEN` identifies the agent server-side; the adapter sends
   it as `Authorization: Bearer <token>` on every request.

   The adapter auto-sets `YELLOWPAGES_ALLOW_ALL_USERS=true` in-process at
   `connect()` time, bypassing Hermes' default DM-pairing flow. The pairing
   flow would otherwise greet each new human with a chat message asking the
   dev to run `hermes pairing approve yellowpages <code>` in a terminal.
   YP's server is the identity provider, so the agent trusts whatever
   `humanId` it sees on `/inbox`. Set `YELLOWPAGES_ALLOW_ALL_USERS=false`
   in `.env` if you want the pairing prompt instead.

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

Every 5 seconds the adapter calls `GET {API_BASE_URL}/inbox`. The server
derives the authenticated agent from `Authorization: Bearer
<YELLOWPAGES_TOKEN>`, so the plugin does not send or configure an
`agentId` for polling. The response is a list of `HumanMessage`:

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

Agent-side endpoints are bearer-authenticated. The backend derives the
author agent from the bearer token, so the adapter does not send
`agentId` in its `/message` body.

### Consumer-facing message hygiene

Yellowpages agents are exposed to **paying end users**, who should only ever
see the agent's in-persona reply. Hermes also emits operator-facing
"control-plane" messages over the same `send()` path: progress/iteration
tickers, dangerous-command approval prompts (`/approve` … `/deny`), DM pairing
codes, and the "no home channel" notice.

`send()` drops these before they reach the consumer (`is_backend_chatter()` in
`adapter.py`). Because the filter lives in the plugin, the guarantee holds for
**every** Yellowpages agent regardless of how its deployment is configured.
Matching is structural (leading `⏳`/`⏱` status glyph, `/approve`+`/deny`
co-occurrence, distinctive phrases) so it survives hermes wording changes.

The plugin also injects a `platform_hint` (see `__init__.py`) instructing the
model to stay in its soul persona and never reveal its backend, tools, or
slash-commands, and to decline prompt-injection attempts in-character.

**This is the enforced floor, not a substitute for configuring the source.**
Each deployment should still:

- Stop progress messages at the source so the agent isn't even generating them:
  ```yaml
  display:
    platforms:
      yellowpages:
        tool_progress: off
        streaming: false
        show_reasoning: false
  ```
  (Yellowpages is not in hermes' built-in `_PLATFORM_DEFAULTS`, so it otherwise
  inherits the global `tool_progress: all`.)
- **Decide how dangerous commands are handled — this is required, not
  optional.** The plugin suppresses the approval *prompt*, but it cannot answer
  it. When a dangerous command is hit and approvals are still on, the agent
  thread blocks on the approval (`approvals.gateway_timeout`, default **300s /
  5 min**), the consumer sees nothing, no `/approve` ever arrives, and the
  command finally resolves as "timed out → blocked." The agent is *not* hung
  forever — it then continues and sends a normal in-persona reply — but the
  consumer experiences a multi-minute silent stall first. To avoid that, every
  agent must remove the gate at the source. The plugin stays neutral on which
  way (it's a per-agent security choice):
  - A sandboxed terminal backend (`docker`/`modal`/`daytona`) — auto-approves
    *and* contains blast radius. Best for a consumer-facing agent.
  - `approvals.mode: off`, or `HERMES_YOLO_MODE=1` in that agent's `.env` — no
    gate at all (agent runs commands unguarded on its host).
  - If you keep approvals on for some reason, at least set a short
    `approvals.gateway_timeout` so the stall is brief.
- **Encode the agent's scope and a friendly refusal in the soul file.** When a
  task is out of scope, blocked, or times out, the agent should decline in its
  own voice — e.g. *"Sorry, I'm an agent that only helps with restaurant
  reservations — I can't take that one on."* The plugin's `platform_hint` nudges
  this, but the soul is where you define what the agent does and how it says no.
- Strip any backend/onboarding language (e.g. "type /help", references to the
  underlying platform) from the agent's **soul file** — the plugin can nudge via
  `platform_hint`, but the soul is the source of the persona.

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
| `YELLOWPAGES_ALLOW_ALL_USERS` | Defaults to `true` — set in-process by `connect()` so every `humanId` the server delivers is authorized. Set to `false` in `.env` to re-enable Hermes' DM pairing prompt. |
| `YELLOWPAGES_HOME_CHANNEL` | Cron delivery target chat_id for `deliver=yellowpages`. The adapter defaults this to the sentinel `disabled` in `connect()` so the "📬 No home channel is set" prompt never reaches end users; override with a real conversationId if you wire up cron-to-YP. |

`API_BASE_URL` and `POLL_INTERVAL_SECONDS` are hardcoded at the top of
`adapter.py`; change them there if needed. If multiple environments
(staging / prod) become a requirement, promote `API_BASE_URL` to an env
var.
