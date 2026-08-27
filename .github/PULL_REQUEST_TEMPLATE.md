<!-- Thanks for contributing to VERA. Keep the change focused and explain the why. -->

## What and why

<!-- What does this change do, and why is it needed? Link any related issue (Closes #123). -->

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Refactor / internal
- [ ] Documentation
- [ ] Build / CI / infrastructure

## Checklist

- [ ] `make check` passes (ruff, pyright, import-linter, unit tests)
- [ ] `make test-int` passes if the data plane changed
- [ ] Added or updated tests for the change
- [ ] Added an Alembic migration if the schema changed (`alembic check` reports no drift)
- [ ] Updated the README or docs for any public interface change
- [ ] No secrets, credentials, or vendor-locked services introduced
