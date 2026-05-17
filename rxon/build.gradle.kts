plugins {
    id("java-library")
    id("maven-publish")
    id("signing")
    alias(libs.plugins.sonatype.central.publisher)
}

group = "com.benaether"
version = "0.3.0-alpha5"

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}

dependencies {
    api(libs.rxjava)
    api(libs.annotation)
    api(libs.rxjava.extensions)

    testImplementation(libs.junit)
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            
            pom {
                name.set("RxOn")
                description.set("A semantic wrapper over RxJava 3 for Java and Android.")
                url.set("https://github.com/abdelmadjid-dev/rxon")
                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }
                developers {
                    developer {
                        id.set("abdelmadjid-dev")
                        name.set("Abdelmadjid")
                        email.set("abdelmadjid.dev@gmail.com")
                    }
                }
                scm {
                    connection.set("scm:git:git://github.com/abdelmadjid-dev/rxon.git")
                    developerConnection.set("scm:git:ssh://github.com/abdelmadjid-dev/rxon.git")
                    url.set("https://github.com/abdelmadjid-dev/rxon")
                }
            }
        }
    }
}

signing {
    val signingKey = project.findProperty("signingKey")?.toString()
    val signingPassword = project.findProperty("signingPassword")?.toString()
    if (signingKey != null && signingPassword != null) {
        useInMemoryPgpKeys(signingKey, signingPassword)
        sign(publishing.publications["mavenJava"])
    }
}

centralPortal {
    username = project.findProperty("ossrhUsername")?.toString()
    password = project.findProperty("ossrhPassword")?.toString()

    publishingType.set(net.thebugmc.gradle.sonatypepublisher.PublishingType.AUTOMATIC)

    pom {
        name.set("RxOn")
        description.set("A semantic wrapper over RxJava 3 for Java and Android.")
        url.set("https://github.com/abdelmadjid-dev/rxon")
        licenses {
            license {
                name.set("The Apache License, Version 2.0")
                url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
            }
        }
        developers {
            developer {
                id.set("abdelmadjid-dev")
                name.set("Abdelmadjid B.")
                email.set("abdelmadjid.dev@gmail.com")
            }
        }
        scm {
            connection.set("scm:git:git://github.com/abdelmadjid-dev/rxon.git")
            developerConnection.set("scm:git:ssh://github.com/abdelmadjid-dev/rxon.git")
            url.set("https://github.com/abdelmadjid-dev/rxon")
        }
    }
}
