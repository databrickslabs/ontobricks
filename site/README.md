# GitHub Pages cutover

Run this only after the branch containing `site/` is merged to `master`.

1. Verify the current Pages configuration:

   ```bash
   gh api repos/databrickslabs/ontobricks/pages
   ```

2. Switch Pages to `master` and `/site`:

   ```bash
   gh api -X PUT repos/databrickslabs/ontobricks/pages \
     -f build_type=legacy \
     -f source[branch]=master \
     -f source[path]=/site
   ```

3. If GitHub rejects the nested API fields, open **Settings → Pages** and set:
   - Source: **Deploy from a branch**
   - Branch: `master`
   - Folder: `/site`

4. Confirm https://databrickslabs.github.io/ontobricks/ serves the new home page
   (deployment may take a minute).
