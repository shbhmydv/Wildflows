# Live toy run

From the WILDFLOWS checkout, with `pi` authenticated, the local OpenAI-compatible
server listening on `127.0.0.1:8080`, and `<target>` a clean Git repository:

```bash
python3 -m wildflows run examples/toy-run/job.md --repo <target>
```

The command prints the generated run id. Resume the same durable run (and optionally
answer a parked Ask) with:

```bash
python3 -m wildflows resume examples/toy-run/job.md --repo <target> --run-id <id> --answer 'yes'
```
