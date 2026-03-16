# How far before expiry each layer triggers a refresh.
# Background > Startup > Inline so that background refresh handles most cases,
# startup catches tokens that expired while the process was down, and
# inline is the last-resort safety net.
BACKGROUND_REFRESH_BUFFER_SECONDS = 300
STARTUP_EXPIRY_BUFFER_SECONDS = 120
INLINE_REFRESH_BUFFER_SECONDS = 30
