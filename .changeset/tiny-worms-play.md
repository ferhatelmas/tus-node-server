---
"@tus/server": patch
---

Stop hanging when a request body stream errors, and treat that failure as an aborted request instead of exposing runtime error details.
