#!/usr/bin/env bash
# One-time deploy script for Railway backend + GitHub Pages frontend.
# Run from the repo root.

set -e

RAILWAY="$HOME/bin/railway"

# ── 1. Railway: install CLI if missing ────────────────────────────────────────
if ! command -v "$RAILWAY" &>/dev/null; then
  echo "Installing Railway CLI..."
  mkdir -p "$HOME/bin"
  bash <(curl -fsSL https://railway.app/install.sh) -b "$HOME/bin" -y
fi

export PATH="$HOME/bin:$PATH"

# ── 2. Railway: login (opens browser once) ────────────────────────────────────
if ! railway whoami &>/dev/null 2>&1; then
  echo "Opening Railway login in browser..."
  railway login
fi

# ── 3. Railway: create project and deploy ─────────────────────────────────────
if [ ! -f ".railway/config.json" ]; then
  railway init --name "1c-matching-api"
fi

echo "Deploying backend to Railway..."
railway up --detach

# ── 4. Get the Railway URL and patch app.js ───────────────────────────────────
RAILWAY_URL=$(railway domain 2>/dev/null || echo "")
if [ -n "$RAILWAY_URL" ]; then
  sed -i.bak "s|https://REPLACE_WITH_RAILWAY_URL|https://$RAILWAY_URL|g" web/app.js
  rm -f web/app.js.bak
  echo "✓ Backend URL set: https://$RAILWAY_URL"
else
  echo "⚠ Could not get Railway URL automatically."
  echo "  After deploy, run:"
  echo "  railway domain"
  echo "  Then replace REPLACE_WITH_RAILWAY_URL in web/app.js"
fi

# ── 5. Set password env var ───────────────────────────────────────────────────
echo ""
echo "Set your password on Railway (default: demo2026):"
echo "  railway variables set MATCHING_PASSWORD=<your_password>"

# ── 6. Commit and push to trigger GitHub Pages ────────────────────────────────
echo ""
echo "Pushing to GitHub to trigger Pages deploy..."
git add web/app.js
git diff --cached --quiet || git commit -m "Set Railway backend URL for GitHub Pages"
git push origin main

echo ""
echo "✓ Done!"
echo "  Backend: https://$RAILWAY_URL"
echo "  Frontend: https://$(git remote get-url origin | sed 's/.*github.com[:/]//' | sed 's/\.git$//' | tr '/' '.').github.io/$(basename $(git remote get-url origin) .git)"
