# Storage

Once a record has been ingested and digested, it is [routed](help:routing) to one or more **stores** based on filter expressions. Each store appends the record to its active chunk.

Stores manage the full lifecycle of your data — [routing](help:routing) controls which records arrive, [rotation](help:policy-rotation) controls when chunks are sealed, and [retention](help:policy-retention) controls when old chunks are deleted. Stores are configured in [Settings](help:storage-engines).

Select a storage engine from the sidebar for format details.
