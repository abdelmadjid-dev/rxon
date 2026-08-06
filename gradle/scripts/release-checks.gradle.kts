evaluationDependsOn(":rxon")
val rxonVersion = project(":rxon").version.toString()

tasks.register("verifyRelease") {
    group = "verification"
    description = "Enforces that release artifacts (changelogs, migrations, docs) are present and consistent."

    val currentVersion = rxonVersion
    val rootDir = project.rootDir

    inputs.property("version", currentVersion)
    inputs.dir(rootDir.resolve("changelogs"))
    inputs.dir(rootDir.resolve("migrations"))
    inputs.file(rootDir.resolve("README.md"))

    doLast {
        if (currentVersion == "unspecified") {
             throw GradleException("Project version is 'unspecified'. Ensure version is set in :rxon/build.gradle.kts")
        }
        println("Verifying release integrity for version: $currentVersion")

        // 1. Check Changelog
        val changelogFile = rootDir.resolve("changelogs/v$currentVersion.md")
        if (!changelogFile.exists()) {
            throw GradleException("Missing changelog for version $currentVersion at ${changelogFile.absolutePath}")
        }
        println("✓ Changelog found: ${changelogFile.name}")

        // 2. Check Migration Guide
        val migrationsDir = rootDir.resolve("migrations")
        val migrationGuide = migrationsDir.listFiles()?.find { it.name.contains(currentVersion) }
        if (migrationGuide == null) {
            throw GradleException("No migration guide found for version $currentVersion in ${migrationsDir.absolutePath}")
        }
        println("✓ Migration guide found: ${migrationGuide.name}")

        // 3. Check README.md version consistency
        val readme = rootDir.resolve("README.md")
        if (!readme.exists()) {
            throw GradleException("README.md not found at ${readme.absolutePath}")
        }
        val readmeText = readme.readText()

        // Check installation snippet
        val installSnippet = "implementation(\"com.benaether:rxon:$currentVersion\")"
        if (!readmeText.contains(installSnippet)) {
            throw GradleException("README.md installation snippet is not updated to version $currentVersion. Expected: $installSnippet")
        }
        println("✓ README.md installation snippet is correct.")

        // Check changelog table entry
        val tableEntry = "v$currentVersion"
        if (!readmeText.contains(tableEntry)) {
            throw GradleException("README.md changelog table is missing an entry for $currentVersion.")
        }
        println("✓ README.md changelog table contains $currentVersion.")
    }
}

// Hook into rxon's check task
project(":rxon").tasks.named("check") {
    dependsOn(tasks.named("verifyRelease"))
}

tasks.register("installGitHooks") {
    group = "help"
    description = "Installs Git hooks from gradle/scripts/hooks to .git/hooks"
    
    val hooksDir = file(".git/hooks")
    val sourceDir = file("gradle/scripts/hooks")
    
    doLast {
        if (!hooksDir.exists()) {
            println("No .git directory found. Skipping hook installation.")
            return@doLast
        }
        
        sourceDir.listFiles()?.forEach { hook ->
            val target = hooksDir.resolve(hook.name)
            hook.copyTo(target, overwrite = true)
            target.setExecutable(true)
            println("✓ Installed hook: ${hook.name}")
        }
    }
}
