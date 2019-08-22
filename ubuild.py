from uranium import current_build

current_build.config.set_defaults({
    "package_name": "intake-dal",
    "module": "intake_dal",
})

current_build.packages.install("orbital-core")
from orbital_core.build import bootstrap_build
bootstrap_build(current_build)

current_build.packages.install("pytest")

current_build.packages.install(
    "git+https://github.com/zillow/intake-nested-yaml-catalog.git@tz/use-intake-catalog-types"
    "#egg=intake-nested-yaml-catalog")
current_build.packages.install(
    "git+https://github.com/zillow/intake-parquet/@tz/gather_statistics_kwargs"
    "#egg=intake-parquet")