# Parity

"Parity" is when there are no more messages to read on a particular topic and partition. This means that you're "caught
up". It's denoted by the next offset being equal to the high watermark.

_kcat_, for example, reports this as "Reached end of topic access_logs [5] at offset 12345".
