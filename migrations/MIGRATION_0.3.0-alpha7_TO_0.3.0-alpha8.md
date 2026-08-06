# Migration Guide: v0.3.0-alpha7 to v0.3.0-alpha8

RxOn v0.3.0-alpha8 focuses on project infrastructure and automation. There are no breaking changes to the library's public API in this release.

---

## 1. Automatic Git Hook Installation

The project now automatically manages its Git hooks. When you sync the project with Gradle (or run any build task), the hooks will be installed or updated in your `.git/hooks` directory.

No manual action is required. If you previously ran `./gradlew installGitHooks`, that task has been removed in favor of the automatic system.

---

## 2. Dependency Update

Update your `build.gradle` to reference the new version:

```kotlin
implementation("com.benaether:rxon:0.3.0-alpha8")
```
