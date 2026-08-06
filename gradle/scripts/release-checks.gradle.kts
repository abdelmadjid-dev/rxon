tasks.register("installGitHooks") {
    group = "help"
    description = "Installs Git hooks from gradle/scripts/hooks to .git/hooks"
    
    val hooksDir = project.rootDir.resolve(".git/hooks")
    val sourceDir = project.rootDir.resolve("gradle/scripts/hooks")
    
    doLast {
        if (!hooksDir.exists()) return@doLast
        
        sourceDir.listFiles()?.forEach { hook ->
            val target = hooksDir.resolve(hook.name)
            if (!target.exists() || !target.readBytes().contentEquals(hook.readBytes())) {
                hook.copyTo(target, overwrite = true)
                target.setExecutable(true)
            }
        }
    }
}

// Auto-install hooks
gradle.taskGraph.whenReady {
    val hooksDir = project.rootDir.resolve(".git/hooks")
    val sourceDir = project.rootDir.resolve("gradle/scripts/hooks")
    if (hooksDir.exists() && sourceDir.exists()) {
        sourceDir.listFiles()?.forEach { hook ->
            val target = hooksDir.resolve(hook.name)
            if (!target.exists() || !target.readBytes().contentEquals(hook.readBytes())) {
                hook.copyTo(target, overwrite = true)
                target.setExecutable(true)
                println("✓ Auto-installed/updated Git hook: ${hook.name}")
            }
        }
    }
}
