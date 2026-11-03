---
'@tus/utils': patch
'@tus/s3-store': patch
---

Clean up chunk files and semaphore permits when multipart uploads are interrupted.
Destroying a `StreamSplitter` without an error now emits `chunkError` and removes
its unfinished chunk.
