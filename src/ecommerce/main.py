"""CLI entrypoint for the ecommerce package.

This exposes a `main()` function that is wired in pyproject.toml under
`[project.scripts]`, so it can be invoked via `uv run main` (after sync)
or as a console script if the package is installed.
"""

def main() -> int:
    """Main CLI function."""
    print("databricks_ecommerce: hello from ecommerce.main")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

