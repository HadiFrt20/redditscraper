# wsgi.py
import os
import signal
import atexit


def make_app():
    from app import create_app

    return create_app()


app = make_app()
application = app  # some servers look for this


def _finalize():
    # Import here so unit tests/imports don’t pull manager at module import time
    try:
        from app.manager import get_manager

        m = get_manager()
        # Best-effort: cancel, flush buffers, save checkpoint, brief join
        m.graceful_shutdown(timeout=8.0)
    except Exception:
        # Don’t let shutdown crash because of checkpointing errors
        pass


# Cover both graceful signals and normal interpreter exit
for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGQUIT):
    try:
        signal.signal(sig, lambda *_: _finalize())
    except Exception:
        # Some environments may not allow installing handlers
        pass

atexit.register(_finalize)

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
