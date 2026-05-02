# GitHub Workflows Deployment Guide

This directory contains the GitHub Actions workflows used to build Pixels and publish the `daily-latest` release.

## Workflows in this directory

- `build-pixels.yml`: reusable workflow that builds the project and optionally packages the output.
- `pr-build-check.yml`: runs the reusable build workflow for pull requests targeting `master`.
- `remove-daily-latest.yml`: reusable workflow that removes existing assets from the `daily-latest` release before a new daily build is published.
- `daily-build.yml`: scheduled and manual workflow that calls `remove-daily-latest.yml`, calls `build-pixels.yml`, updates the `daily-latest` tag, publishes the release with `gh release`, and removes old artifacts.

## Required secret

The release-related workflows use a repository secret named `PIXELS_DEVELOP`.

It is currently referenced by:

- `daily-build.yml`
- `remove-daily-latest.yml`

`PIXELS_DEVELOP` is the secret name stored in GitHub.

Inside workflow steps, that same secret value is passed to tools using the variable or input names that those tools expect:

- `GH_TOKEN`: environment variable consumed by the GitHub CLI `gh`
- `github_token`: action input consumed by `c-hive/gha-remove-artifacts`

Example:

```yaml
env:
  GH_TOKEN: ${{ secrets.PIXELS_DEVELOP }}
```

This does not create a second secret. It only maps the repository secret `PIXELS_DEVELOP` to the runtime environment variable name required by `gh`.

## What the token needs to do

`PIXELS_DEVELOP` must be able to:

- read and update releases
- delete release assets
- create and move the `daily-latest` tag
- remove old GitHub Actions artifacts

In practice, use one of these options:

1. Fine-grained personal access token for this repository with at least:
   `Contents: Read and write`
   `Actions: Read and write`
2. Classic personal access token with `repo` scope

If your organization restricts PAT usage, use the option that your GitHub organization allows.

## How to create the secret

### Option 1: Create a fine-grained personal access token

1. In GitHub, open `Settings` for the account that will own the token.
2. Open `Developer settings`.
3. Open `Personal access tokens` -> `Fine-grained tokens`.
4. Click `Generate new token`.
5. Set:
   - Token name: `pixels-daily-build` or similar
   - Resource owner: the owner of this repository
   - Repository access: `Only select repositories`, then select this repository
   - Repository permissions:
     - `Contents`: `Read and write`
     - `Actions`: `Read and write`
6. Choose an expiration that matches your security policy.
7. Generate the token and copy it immediately.

### Option 2: Create a classic personal access token

1. In GitHub, open `Settings` for the account that will own the token.
2. Open `Developer settings`.
3. Open `Personal access tokens` -> `Tokens (classic)`.
4. Click `Generate new token`.
5. Grant the `repo` scope.
6. Generate the token and copy it immediately.

## How to add the token as `PIXELS_DEVELOP`

### Repository secret in the GitHub UI

1. Open the repository on GitHub.
2. Go to `Settings` -> `Secrets and variables` -> `Actions`.
3. Open the `Secrets` tab.
4. Click `New repository secret`.
5. Set:
   - Name: `PIXELS_DEVELOP`
   - Secret: paste the token value
6. Click `Add secret`.

### Repository secret with GitHub CLI

```bash
gh secret set PIXELS_DEVELOP
```

GitHub CLI will prompt for the token value.

## First-time verification

After adding the secret:

1. Open `Actions` in the repository.
2. Run `Pixels Daily Build and Release` with `Run workflow`.
3. Confirm that:
   - the `remove` job can delete old `daily-latest` assets
   - the `release` job can update the `daily-latest` tag
   - the workflow can publish the new `daily-latest` release
   - the `cleanup-artifact` job can remove old artifacts

## Common failure modes

- `Resource not accessible by integration`:
  the token does not have enough permissions, or the organization blocks the token type.
- `secret not found`:
  the secret name is not exactly `PIXELS_DEVELOP`.
- release deletion or tag update fails:
  the token owner does not have push-level access to this repository.

## References

- GitHub Docs: Using secrets in GitHub Actions
  https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets
- GitHub Docs: Managing your personal access tokens
  https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token
- GitHub Docs: Permissions required for fine-grained personal access tokens
  https://docs.github.com/en/rest/authentication/permissions-required-for-fine-grained-personal-access-tokens
