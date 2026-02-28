name: Build

on:
  pull_request:
  push:
    branches: ["main"]
  workflow_dispatch:

env:
  XMAKE_GLOBALDIR: ${{ github.workspace }}/.xmake-global
  XMAKE_PKG_CACHEDIR: ${{ github.workspace }}/.xmake-cache

jobs:
  build:
    runs-on: windows-latest
    steps:
      - uses: actions/checkout@v6

      - name: Setup xmake
        uses: xmake-io/github-action-setup-xmake@v1
        with:
          xmake-version: latest

      # ── 计算缓存 key ──
      - name: Hash sources
        id: hashes
        run: |
          $srcHash = (Get-FileHash -Algorithm SHA256 -InputStream (
            [IO.MemoryStream]::new([Text.Encoding]::UTF8.GetBytes(
              (Get-ChildItem -Recurse -Include *.cpp,*.h,*.hpp -Path src | Get-Content -Raw) + (Get-Content xmake.lua -Raw)
            ))
          )).Hash.Substring(0, 16)
          $depsHash = (Get-FileHash -Algorithm SHA256 -Path xmake.lua).Hash.Substring(0, 16)
          echo "src=$srcHash" >> $env:GITHUB_OUTPUT
          echo "deps=$depsHash" >> $env:GITHUB_OUTPUT
        shell: pwsh

      # ── 缓存 xmake 包（最大瓶颈）──
      # 只要 xmake.lua 不变，依赖包不会变
      - name: Cache xmake packages
        uses: actions/cache@v4
        id: cache-packages
        with:
          path: |
            ${{ env.XMAKE_GLOBALDIR }}
            ${{ env.XMAKE_PKG_CACHEDIR }}
            ${{ env.LOCALAPPDATA }}\.xmake
          key: pkgs-${{ steps.hashes.outputs.deps }}
          restore-keys: |
            pkgs-
          save-always: true

      # ── 缓存构建中间产物（.obj）──
      # 源码不变 = 编译结果不变
      - name: Cache build
        uses: actions/cache@v4
        id: cache-build
        with:
          path: build
          key: build-${{ steps.hashes.outputs.src }}
          restore-keys: |
            build-
          save-always: true

      - name: Cache status
        run: |
          echo "Packages cache: ${{ steps.cache-packages.outputs.cache-hit }}"
          echo "Build cache: ${{ steps.cache-build.outputs.cache-hit }}"

      # ── Configure（仅在需要时）──
      - name: Configure
        if: steps.cache-build.outputs.cache-hit != 'true'
        run: xmake f -a x64 -m release -p windows -y

      # ── 构建 ──
      - name: Build
        run: |
          # 确保配置存在（缓存命中但配置丢失的情况）
          if (-not (Test-Path "build\.gens")) {
            xmake f -a x64 -m release -p windows -y
          }
          xmake -y -j $env:NUMBER_OF_PROCESSORS
        shell: pwsh

      # ── 产物 ──
      - uses: actions/upload-artifact@v4
        with:
          name: ${{ github.event.repository.name }}-windows-x64
          path: bin/
