<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Installation of Podman Desktop

## Source to download

[Podman](https://podman-desktop.io/downloads)

## Make sure that the Windows Linux Subsystem (WSL) is installed

Podman Desktop uses Linux in the background.

## Run in terminal

After Podman Desktop has been installed, the further actions
can be taken from the GUI or via CLI.

### To fetch mssql image and accept eula

1. podman pull mcr.microsoft.com/mssql/server:2022-latest
2. podman run
   -e 'ACCEPT_EULA=Y’
   -e ‘MSSQL_SA_PASSWORD=Password123’
   -p 1433:1433
   --name mssql
   -d mcr.microsoft.com/mssql/server:2022-latest

## Create a container in podman using the mssql image

The Podman Desktop will guide you through it.

## Important

Podman Desktop should automatically start with Windows
The database container must be started manually
