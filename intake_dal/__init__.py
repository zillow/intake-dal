# to avoid circular dependency: importing the package imports intake, which tries
# to discover and import packages that define drivers. To avoid this pitfall,
# just ensure that ``intake`` is imported first thing in your package's  ``__init__.py``
import intake  # noqa: F401
