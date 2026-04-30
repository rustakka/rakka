# rakka-dashboard UI

React + Vite + TypeScript single-page app for the rakka dashboard
backend (`crates/rakka-dashboard`).

## Dev loop

```bash
pnpm install
pnpm dev          # Vite on :5173, proxies /api + /ws to 127.0.0.1:9100
```

Start the backend in another terminal:

```bash
cargo run -p rakka-dashboard --features bin --bin rakka-dashboard \
  -- --bind 127.0.0.1:9100
```

Or run both together:

```bash
./scripts/dashboard-dev.sh      # runs cargo (+ cargo-watch if installed) + pnpm dev
```

## Production bundle

```bash
pnpm build
cargo build -p rakka-dashboard --features embed-ui
```

## Mobile layout

- `< md` breakpoint collapses the sidebar to a bottom tab bar with the
  five most important sections.
- Cards stack to one column and long tables scroll horizontally.
- React Flow canvases use full-viewport height on mobile.
