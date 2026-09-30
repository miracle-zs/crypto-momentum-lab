"""Strategy execution modules. Import use cases from their owning modules.

Package initialization must not load Postgres or dataset adapters when a
pure exit, fill or position calculation is imported.
"""
