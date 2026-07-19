# 0006 — Publish everything under one namespace, from one repository

Status: **Accepted**, supersedes the Quarkiverse-donation assumption in ADR-era planning

## Context

The Quarkus extension originally used Java packages under `io.quarkiverse.sftp.client.*` while its
Maven coordinates were already `io.github.jtsirogiannos`. That mismatch was deliberate, and the
reasoning was recorded in the module's POM: donating the extension to Quarkiverse would land it in
`io.quarkiverse.*`, and renaming packages after a public release breaks every consumer, so the
packages were named for their intended destination up front.

That reasoning was sound while donation was the plan. It is a bet, and the bet has been called off:
the Quarkus extension and the Spring starters are now treated as permanent parts of this project.
Publishing happens under the author's own handle, with a move into a GitHub organisation as a
possible later step.

Under the new premise the trade inverts, and every branch favours renaming:

- **Rename now** — free. Nothing is published; there are no consumers to break.
- **Rename after the first release** — breaking, exactly as the original comment warned.
- **Never rename, and donation never happens** — classes ship permanently in a namespace we do not
  own. If Quarkiverse ever publishes its own `io.quarkiverse.sftp.*` the result is a split package,
  and in the meantime every user's import list implies this is an official Quarkiverse extension.
- **Rename now, and donation happens anyway** — costs nothing extra. Quarkiverse mandates groupId
  `io.quarkiverse.<repo-name>`, so donation forces a breaking coordinate change regardless; the
  package rename rides along with a break that was happening either way.

## Decision

Java packages become `io.github.jtsirogiannos.sftp.client.quarkus.{runtime,deployment,it}`, matching
the Spring modules' `...sftp.client.spring`. Coordinates stay `io.github.jtsirogiannos`.

One repository, one version, one coordinate space, indefinitely. A tagged release publishes every
module together. The extension gains the `META-INF/quarkus-extension.yaml` descriptor it needs to
appear in the Quarkus catalog and dev tools, which a donated extension would have received from the
Quarkiverse parent POM.

Only `io.quarkiverse.sftp.*` was renamed. `io.quarkiverse.jsch:quarkus-jsch` and
`io.quarkiverse.sshd:quarkus-sshd` are real upstream artifacts and are untouched.

## Consequences

- **A Spring-only fix bumps the version of the core**, because everything releases together. That is
  the monorepo trade; Spring Boot makes the same one. What it buys is that an SPI change never needs
  SNAPSHOT juggling across three builds, and the framework modules cannot rot against a core that
  moved without them.
- **The framework modules need their own early warning.** Owning the Quarkus and Spring integrations
  permanently means learning that an upstream release broke them before a user does. That is a
  scheduled compatibility matrix over `-Dquarkus.version` and `-Dspring-boot.version`, on a different
  clock from the per-PR build — which is the one place where splitting CI per domain genuinely pays.
- **Moving to a GitHub organisation later needs nothing from the build.** A repository transfer
  preserves history, issues and Actions; only `<scm>`, `<url>` and badge links change.
- **Donation stays possible but gets more expensive over time**, since it forces a groupId change
  that breaks existing adopters. That decision now has to be made before the extension has users.
