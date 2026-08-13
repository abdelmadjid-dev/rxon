plugins {
    alias(libs.plugins.android.application) apply false
    alias(libs.plugins.android.library) apply false
}

// Ensure Git uses the versioned .githooks folder for commit/push verification.
val setupGitHooks = tasks.register<Exec>("setupGitHooks") {
    group = "help"
    description = "Configures Git to use the versioned .githooks directory."
    commandLine("git", "config", "core.hooksPath", ".githooks")
}

// Configure git hooks path during evaluation in a configuration-cache-safe way
if (file(".git").exists()) {
    providers.exec {
        commandLine("git", "config", "core.hooksPath", ".githooks")
    }.result.orNull
}
