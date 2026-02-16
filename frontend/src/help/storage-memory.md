# Memory Store

Type: `memory`

Keeps everything in memory. Fast, but data is lost on restart. Useful for development, testing, and throwaway environments. Often paired with the [Chatterbox ingester](help:ingester-chatterbox) for quick experimentation.

| Param | Description | Default |
|-------|-------------|---------|
| `maxRecords` | Maximum records before rotation | `10000` |
