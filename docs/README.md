# GitHub Pages marketing site

Static marketing site for OntoBricks. GitHub Pages **Deploy from a branch**
only allows `/` or `/docs` as the publishing folder — this site lives in
`/docs` for that reason. Product Markdown / Sphinx docs live in
`/documentation`.

## Cutover

Run this only after this `docs/` tree is on the branch Pages publishes
(currently often `develop` or `master` — check Settings → Pages).

1. Verify the current Pages configuration:

   ```bash
   gh api repos/databrickslabs/ontobricks/pages
   ```

2. Switch Pages to publish `/docs` (UI or API):

   ```bash
   gh api -X PUT repos/databrickslabs/ontobricks/pages \
     -f build_type=legacy \
     -f 'source[branch]=develop' \
     -f 'source[path]=/docs'
   ```

   Or open **Settings → Pages** and set:
   - Source: **Deploy from a branch**
   - Branch: your publish branch (`develop` or `master`)
   - Folder: **`/docs`**

3. Confirm https://databrickslabs.github.io/ontobricks/ serves this home page
   (deployment may take a minute).

## Local preview

```bash
cd docs && python -m http.server 8765
# open http://127.0.0.1:8765/
```
