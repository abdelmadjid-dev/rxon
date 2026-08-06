plugins {
    alias(libs.plugins.android.application) apply false
    alias(libs.plugins.android.library) apply false
}

// Ensure Git uses the versioned .githooks folder for commit/push verification.
// This is performed during Gradle configuration (e.g., IDE Sync).
val setupGitHooks = tasks.register<Exec>("setupGitHooks") {
    group = "help"
    description = "Configures Git to use the versioned .githooks directory."
    commandLine("git", "config", "core.hooksPath", ".githooks")
}

// Silently configure git hooks path during build evaluation
if (file(".git").exists()) {
    "git config core.hooksPath .githooks".runCommand(rootDir)
}

fun String.runCommand(workingDir: File) {
    ProcessBuilder(*split(" ").toTypedArray())
        .directory(workingDir)
        .start()
        .waitFor()
}
