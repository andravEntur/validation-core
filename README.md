# validation-core

## Description

This repository contains shared code for Entur's validation platform, published as a Java library.
It contains schema and generated code only.

## Obtain

```shell
git clone git@github.com:entur/validation-core.git
```

## Versioning and publishing

On every push to `main`, CD ([`.github/workflows/cd.yml`](.github/workflows/cd.yml)) bumps the patch version
in [`gradle.properties`](gradle.properties) and publishes both modules to Entur's JFrog Artifactory
(`entur-release-standard`) via [`entur/gha-artifactory`](https://github.com/entur/gha-artifactory).
