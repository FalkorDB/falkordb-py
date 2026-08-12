# Contributing

## Dependency and lockfile policy

- `uv.lock` is for reproducible development and CI environments (`uv sync`). It is
  committed to this repository, but it is not shipped in wheels/sdists and `pip`
  does not read it when users install `falkordb`.
- `pyproject.toml` `dependencies` define what users resolve when they run
  `pip install falkordb`.
- Keep dependency constraints permissive by default. Do not add speculative upper
  bounds; add caps/exclusions only when there is a demonstrated incompatibility.
- CI includes a non-blocking "latest dependencies" test job to exercise newest
  compatible dependency resolutions and catch upstream breakage early.
