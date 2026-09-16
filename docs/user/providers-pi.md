# Pi

Pi is an early-access provider. Install [Pi](https://github.com/badlogic/pi-mono/tree/main/packages/coding-agent) on the machine running your T3 Code environment. Run `pi`, use `/login` to authenticate, and choose a model with `/model`. Then enable Pi in **Settings → Providers**.

Set **Binary path** if `pi` is not on the server's `PATH`. **Pi agent directory** selects a separate Pi configuration, including its credentials, models, and extensions. It is a path on the environment's machine, not the computer displaying T3 Code.

Pi executes tools using its own configuration. Configure approval extensions in Pi if you need permission prompts. Provider-side conversation rewind and Pi-powered automatic title/commit generation are not supported.

## Background subagents

The Agents panel can display Pi children's progress and saved transcripts, and cancel individual children. Finished child results return to the parent automatically. The child transcript and cancellation surface is available in web and desktop, not mobile.

This requires the **T3-compatible subagents companion**, maintained separately from this server. Stock Pi or the stock subagents extension does not supply this integration. Do not load the stock and T3-compatible extensions together.

With a local copy of the companion, install its dependencies and load it into a separate Pi profile:

```bash
cd /absolute/path/to/t3-compatible-subagents
npm install
PI_CODING_AGENT_DIR="$HOME/.pi/t3-agent" pi install "$PWD"
PI_CODING_AGENT_DIR="$HOME/.pi/t3-agent" pi
```

Authenticate and select a model in that profile, then set T3 Code's **Pi agent directory** to its absolute path. Restart the thread's Pi session after changing extensions. This leaves your normal Pi profile unchanged.

Cancelling one child does not stop its siblings. Stopping the parent retires its Pi process and stops its outstanding children; later user work resumes the saved conversation in a fresh process. A child left unfinished by a process or server restart is shown as interrupted, not still running.
