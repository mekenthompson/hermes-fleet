# Webkite web provider

The Fleet image bundles a generic Webkite CLI provider. It implements `web_search` and URL extraction through a local `webkite` binary.

The provider is unavailable unless the runtime can execute `webkite` on `PATH`. The image contains no API key, secret reference, profile binding, or deployment routing.

Select it with the Hermes configuration keys:

```yaml
web:
  search_backend: webkite
  extract_backend: webkite
```

The provider is disabled by default. Deployments that want a different search or extract backend leave these keys unset or point them at another bundled provider.
