# Deploy to Render (Worker, polling)

## What this repo expects
- `Botv2.py` — entrypoint (already configured to use POLLING when `BASE_URL` is absent).
- `requirements.txt` — Python deps.
- `data/` folder with:
  - `faq.xlsx` (or set env `FAQ_XLSX_PATH` to a different path in the repo)
  - optional attachments used by `/post` and FAQ

## Quick steps (GUI)
1. Push this repo to GitHub.
2. On https://render.com → **New → Blueprint** (or **Background Worker**):
   - If using **Blueprint**, Render will read `render.yaml` (this file).
   - If using **Background Worker**, set:
     - Build Command: `pip install -r requirements.txt`
     - Start Command: `python Botv2.py`
3. Add env vars (Settings → Environment):
   - **BOT_TOKEN** — Telegram bot token (required)
   - **POST_ADMINS** — comma-separated Telegram user IDs
   - **TARGET_CHAT_ID** — target group/channel id (e.g., `-1001234567890` or `@channelusername`)
   - **TARGET_THREAD_ID** — (optional) topic/thread id in the target chat
   - **SUGGEST_CHAT_ID** or **SUGGEST_ADMINS** — where to notify about suggestions
   - **AUDIT_CHAT_ID** — (optional) chat for audit messages
   - **FAQ_XLSX_PATH** — (optional) custom path to your Excel, default is `data/faq.xlsx`
4. Deploy. No `BASE_URL` is set ⇒ the bot uses **polling** (no HTTP port required).

## Notes
- If you previously enabled webhook on this token, ensure it's cleared. The bot already calls
  `delete_webhook(drop_pending_updates=True)` at startup.
- Keep `data/faq.xlsx` and files for posting inside the repo so Render can access them at runtime.
- If you want webhook mode later, deploy as a Web Service and set `BASE_URL` (public Render URL).