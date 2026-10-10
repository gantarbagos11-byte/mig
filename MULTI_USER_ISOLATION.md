# Multi-user session isolation

This build isolates each authenticated browser session:

- WS1-WS10 are registered under the login session that owns them.
- KICK ALL selects only that session's upstream sockets.
- A kick job is keyed by `(session_id, room)`, so two users may run jobs in the same room without sharing job state.
- Confirmed-kick events update only the job owned by the session whose upstream connection received the event.
- Kick progress and authoritative target events are sent only to browsers in the same login session.
- Backend KICK ALL also removes targets matching the current session's own WS usernames, so the browser cannot bypass its own-WS protection.
- The other user's WS usernames are not globally blocked; this preserves intentional user-vs-user testing/dueling. If global protection is desired later, it can be enabled separately.
