# Public Fleet sidecar images

Hermes Fleet publishes sidecar images from this repository to GHCR. They are
not baked into the agent child image. Deployments pin an immutable digest.

## Packages

| Sidecar | Image | Status |
|---|---|---|
| REST lock-proxy | `ghcr.io/mekenthompson/hermes-fleet-rest-lock-proxy` | publishing |
| TCP / noVNC proxy | `ghcr.io/mekenthompson/hermes-fleet-tcp-proxy` | publishing |
| Browser broker | `ghcr.io/mekenthompson/hermes-fleet-browser-broker` | publishing |
| Kokoro | `ghcr.io/mekenthompson/hermes-fleet-kokoro` | publishing |
| Camofox | `ghcr.io/mekenthompson/hermes-fleet-camofox` | planned |

Pin form:

```text
ghcr.io/mekenthompson/hermes-fleet-rest-lock-proxy@sha256:<64 lowercase hex>
```

`scripts/sidecar_image_ref.py` rejects tags and unprefixed legacy names.

## Layout

- `sidecars/packages.json` is the machine-readable package list. It is not
  copied into the Fleet child image, so sidecar-only changes do not rebuild
  `hermes-fleet-public`.
- Source lives under `sidecars/<sidecar>/`.
- Path-filtered workflows under `.github/workflows/sidecar-*.yml` bake on this
  repository only.

## Out of scope

Household compose, identities, secret names, and live digest pins stay in the
private deployment overlay. This tree ships generic images and synthetic
examples only. Browser-broker chrome reads `BRAND`, `LOCAL_TZ`, and a required
matching `PUBLIC_BASE`/`PUBLIC_ORIGIN` from the environment.

Kokoro is a CUDA runtime image. Model weights are not baked into the image.
