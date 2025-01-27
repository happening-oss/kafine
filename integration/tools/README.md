## Deleting all topics

This will delete *everything*; make sure you're extremely careful with it.

```
ERL_LIBS=deps tools/list_topics.escript localhost:9092 | \
    ERL_LIBS=deps xargs -r tools/delete_topics.escript localhost:9092
```
