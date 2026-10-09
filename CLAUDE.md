# My Redis - Instructions for Claude Code

## Project
A simplified Redis clone in Python: asyncio TCP server, RESP2 protocol, in-memory storage.
Goal: finish the project AND help me (Arti) understand it well enough for placement interviews.

## Rules
- Server code uses only the Python standard library. (redis and pytest are allowed only for tests.)
- Keep code simple and readable. Clarity over cleverness. Short comments on important logic.
- Server runs on 127.0.0.1:6380.
- Files: server.py, resp.py, commands.py, storage.py, persistence.py, pubsub.py, client_state.py, config.py, client.py, benchmark.py, tests/
- Work one step at a time. Do not implement anything I have not asked for yet.

## How to explain (very important)
Before code: explain what you will implement and why (short).
After code: explain only the important parts, not line by line:
- Library/Module -> Where used -> Why used
- Data Structure -> Where used -> Why used -> Why suitable
- Functionality -> How it works -> Concepts -> Possible interview questions
- Our implementation vs Real Redis
- Limitations of this step
Then give me exact commands to test the step manually.
I prefer casual Hindi-English explanations and direct, honest feedback.
