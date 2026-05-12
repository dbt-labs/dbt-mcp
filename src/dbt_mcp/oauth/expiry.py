# Named constants for OAuth token expiry buffers (in seconds).
#
# Each constant controls how far *before* the token's ``expires_at`` a
# particular code-path considers the token "expired" and triggers a refresh.

# Background worker: refresh well before the token actually expires so that
# callers almost never see an expired token.
BACKGROUND_REFRESH_BUFFER_SECONDS = 300  # 5 minutes

# Startup check: used when validating a cached token at server boot.
STARTUP_EXPIRY_BUFFER_SECONDS = 120  # 2 minutes

# Inline (synchronous) refresh: last-resort safety net inside ``get_token()``.
INLINE_REFRESH_BUFFER_SECONDS = 30
