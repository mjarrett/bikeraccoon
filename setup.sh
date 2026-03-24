#!/usr/bin/env bash
# Sets up br-tracker on a fresh machine.
# Usage: sudo DEPLOY_USER=exedev BIKERACCOON_REPO=https://github.com/... bash <(curl -fsSL https://raw.githubusercontent.com/.../setup.sh)
# Or:    git clone $BIKERACCOON_REPO /srv/bikeraccoon && sudo DEPLOY_USER=exedev /srv/bikeraccoon/setup.sh
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────────
DEPLOY_USER="${DEPLOY_USER:-${SUDO_USER:-$USER}}"
BIKERACCOON_REPO="https://github.com/mjarrett/bikeraccoon"
BIKERACCOON_DIR="/srv/bikeraccoon"
TRACKER_DIR="/srv/br-tracker"
VENV="$TRACKER_DIR/.venv"

# ── Helpers ───────────────────────────────────────────────────────────────────
info() { echo "==> $*"; }
die()  { echo "ERROR: $*" >&2; exit 1; }

[[ $EUID -eq 0 ]] || die "Run as root: sudo $0"
id "$DEPLOY_USER" &>/dev/null || die "User '$DEPLOY_USER' does not exist"

# ── System packages ───────────────────────────────────────────────────────────
info "Installing system packages..."
apt-get update -qq
apt-get install -y -qq git curl

# ── Install uv ────────────────────────────────────────────────────────────────
if ! command -v uv &>/dev/null; then
    info "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | UV_INSTALL_DIR=/usr/local/bin sh
fi

# ── bikeraccoon repo ──────────────────────────────────────────────────────────
info "Setting up bikeraccoon at $BIKERACCOON_DIR..."
if [[ -d "$BIKERACCOON_DIR/.git" ]]; then
    git -C "$BIKERACCOON_DIR" pull origin master
else
    git clone "$BIKERACCOON_REPO" "$BIKERACCOON_DIR"
fi
chown -R "$DEPLOY_USER:$DEPLOY_USER" "$BIKERACCOON_DIR"

# ── br-tracker working directory ──────────────────────────────────────────────
info "Setting up working directory at $TRACKER_DIR..."
install -d -o "$DEPLOY_USER" -g "$DEPLOY_USER" "$TRACKER_DIR"
install -d -o "$DEPLOY_USER" -g "$DEPLOY_USER" "$TRACKER_DIR/logs"
install -d -o "$DEPLOY_USER" -g "$DEPLOY_USER" "$TRACKER_DIR/tracker-data"
install -d -o root -g root -m 755 /var/lib/br-tracker

# ── systems.json ──────────────────────────────────────────────────────────────
info "Setting up systems.json..."
if [[ -f "$TRACKER_DIR/systems.json" ]]; then
    info "  systems.json already exists, skipping (edit $TRACKER_DIR/systems.json manually)"
else
    install -o "$DEPLOY_USER" -g "$DEPLOY_USER" -m 640 \
        "$BIKERACCOON_DIR/systems.json.example" "$TRACKER_DIR/systems.json"
    info "  installed systems.json from systems.json.example — edit $TRACKER_DIR/systems.json before starting services"
fi

# ── Environment file ──────────────────────────────────────────────────────────
info "Setting up environment file..."
if [[ -f "$TRACKER_DIR/.env" ]]; then
    info "  .env already exists, skipping (edit $TRACKER_DIR/.env manually)"
else
    install -o "$DEPLOY_USER" -g "$DEPLOY_USER" -m 600 \
        "$BIKERACCOON_DIR/.env.example" "$TRACKER_DIR/.env"
    info "  installed .env from .env.example — edit $TRACKER_DIR/.env before starting services"
fi

# ── Python virtual environment ────────────────────────────────────────────────
info "Creating virtual environment at $VENV..."
sudo -u "$DEPLOY_USER" uv venv --python 3.12 "$VENV"
sudo -u "$DEPLOY_USER" uv --directory $VENV pip install -q \
    -e "$BIKERACCOON_DIR" \
    gunicorn

# ── Notify script ─────────────────────────────────────────────────────────────
info "Installing notify script..."
install -m 755 "$BIKERACCOON_DIR/bin/br-tracker-notify" /usr/local/bin/br-tracker-notify

# ── systemd services ──────────────────────────────────────────────────────────
info "Installing systemd services..."
for svc in br-tracker br-api br-dash; do
    sed "s/__DEPLOY_USER__/$DEPLOY_USER/g" "$BIKERACCOON_DIR/systemd/${svc}.service" \
        > "/etc/systemd/system/${svc}.service"
    info "  installed ${svc}.service"
done
systemctl daemon-reload
for svc in br-tracker br-api br-dash; do
    systemctl enable --now "$svc"
done

# ── sudoers: passwordless systemctl restart for CI deploys ────────────────────
info "Configuring sudoers..."
SUDOERS_FILE=/etc/sudoers.d/br-tracker
cat > "$SUDOERS_FILE" << EOF
# Allow $DEPLOY_USER to restart br-tracker services (for CI deploys)
$DEPLOY_USER ALL=(ALL) NOPASSWD: /bin/systemctl restart br-tracker br-api br-dash
$DEPLOY_USER ALL=(ALL) NOPASSWD: /bin/systemctl start br-tracker br-api br-dash
$DEPLOY_USER ALL=(ALL) NOPASSWD: /bin/systemctl stop br-tracker br-api br-dash
EOF
chmod 440 "$SUDOERS_FILE"
visudo -cf "$SUDOERS_FILE"

# ── Done ──────────────────────────────────────────────────────────────────────
info ""
info "Setup complete. Next steps:"
info "  1. Edit $TRACKER_DIR/.env — fill in SMTP credentials, admin key, etc."
info "  2. Edit $TRACKER_DIR/systems.json — add the systems you want to track"
info "  3. Add GitHub Actions secrets: DEPLOY_HOST, DEPLOY_USER, DEPLOY_SSH_KEY"
info "  4. Add the deploy user's public key to ~/.ssh/authorized_keys"
