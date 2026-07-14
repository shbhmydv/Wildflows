# Dashboard status (v2)

The frame architecture changed the journal vocabulary and control model. The current
`python3 -m wildflows dash --repo <target>` entrypoint intentionally serves only a
small v2 backend: run listing, run detail, frame count, pending questions, and raw v2
events. It does not attempt to render the removed v1 expression/epoch tree.

A frame-aware control room (live call stack, frame results, gates, asks, replay state,
and controls) is the next phase. The journal remains its only durable data source.
