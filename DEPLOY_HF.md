# Deploying PokeDelta to Hugging Face Spaces

Everything is pre-configured. Tomorrow morning, run these steps once.

---

## Step 1 — Create HuggingFace account (3 min)

1. Go to [huggingface.co/join](https://huggingface.co/join). Sign up with
   email. No credit card. Pick a username — that's your handle in URLs.
2. From the top-right avatar menu → **Access Tokens** → **New token**:
   - Name: `pokedelta-deploy`
   - Type: **Write**
   - Copy the token (you'll only see it once).

---

## Step 2 — Create the Space (2 min)

1. Top-right avatar → **+ New Space**.
2. Fill in:
   - **Owner**: your username
   - **Space name**: `pokemon-analytics-delta` (or whatever you want)
   - **License**: MIT
   - **Space SDK**: **Docker** → **Blank Docker**
   - **Hardware**: CPU basic (free)
   - **Visibility**: Public
3. Click **Create Space**. You'll land on an empty repo page.

---

## Step 3 — Install git-lfs (one-time, 1 min)

HF Spaces needs Git LFS for files over 10MB. The SQLite snapshot (226MB)
and LightGBM model artifacts go through LFS automatically thanks to
`.gitattributes`. Install once:

```bash
brew install git-lfs
git lfs install
```

Verify:
```bash
git lfs version   # should print git-lfs/x.x.x
```

## Step 4 — Push from your machine (5 min)

HF Spaces is a git repo. In the terminal on your laptop:

```bash
cd /Users/yoson/pokemon-analytics-delta

# Configure git author (first time only)
git config user.email "your@email.com"
git config user.name "Your Name"

# Hook up the HF remote (replace <username> + <spacename>)
git remote add hf https://huggingface.co/spaces/<username>/pokemon-analytics-delta

# Stage everything including the SQLite snapshot.
# .gitattributes already routes *.db + *.lgb through LFS.
git add -A
git commit -m "deploy: HF Spaces Docker configuration"

# (Optional) Verify the DB is recognized as LFS:
git lfs ls-files | head -5   # should list data/pokemon.db and model .lgb files

# Push. Git will prompt for username + the token you copied in Step 1.
#   Username: <your HF username>
#   Password: <paste the hf_... token>
git push hf master
```

The first push uploads ~330MB (snapshot DB + model files) through LFS.
Expect 2–5 minutes on a decent connection.

That's it. HF will see the push, detect the `Dockerfile`, build the image
(~5–10 min first time), and bring up your Space at:

```
https://huggingface.co/spaces/<username>/pokemon-analytics-delta
```

You can watch the build progress in the Space's **Logs** tab.

---

## What's already prepared (no action needed)

- `Dockerfile` — Python 3.11 + LightGBM runtime, listens on port 7860.
- `.dockerignore` — excludes 1.9GB of `data/cache/` (PC HTML), logs, and
  other dev artifacts. The 233MB `data/pokemon.db` snapshot IS included,
  so the Space serves real data on cold boot.
- `README.md` — HF Spaces frontmatter (sdk: docker, port: 7860).
- `api/main.py` already serves the frontend at `/` and the API at `/api/*`,
  same origin. No CORS surgery needed.
- Config defaults in `config/settings.py` are all empty strings, so nothing
  crashes on startup even without eBay / PC credentials.

---

## If the build fails

Common HF Spaces build issues:

| Symptom | Fix |
|---|---|
| Build times out (10-min cap) | Split the image — base + data layer. Ping me. |
| LightGBM missing libgomp | Already handled via `apt-get install libgomp1` in Dockerfile. |
| "git lfs required" warning on pokemon.db | 233MB is under HF's 500MB per-file limit, so no LFS needed. Push should work. |
| DB locked at runtime | Add `?mode=ro` to the connection string — safe since it's read-only anyway. |

Check the **Logs** tab for the actual error, paste it to me, and I'll patch.

---

## After the first deploy

Every code/data change → commit → `git push hf master` → HF auto-rebuilds.

If you want the data to refresh (new eBay pulls, new projections):

```bash
# Locally, run your pipeline to update data/pokemon.db
# Then push the fresh snapshot:
git add data/pokemon.db
git commit -m "refresh: data snapshot <date>"
git push hf master
```

---

## Sharing the Space with friends

Once it's built: just send them the URL. No login required to view a
public Space. It's fast (served from HF's CDN) and stays up 24/7.

If you get traction, upgrading to **Persistent Storage** ($5/mo) lets
you persist DB writes (paper trading, etc.) without redeploying.
