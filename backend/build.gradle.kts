import org.gradle.api.plugins.JavaPluginExtension
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension

// 版本统一由 gradle/libs.versions.toml 管理；各模块按需引插件与依赖
plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.spring) apply false
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.ktlint) apply false
    alias(libs.plugins.protobuf) apply false
}

allprojects {
    group = "dev.yidafu.flowtrace"
    version = "0.0.1-SNAPSHOT"

    repositories {
        // 国内加速：腾讯云 Maven 镜像优先，Maven Central 兜底
        maven("https://mirrors.cloud.tencent.com/nexus/repository/maven-public/")
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "org.jlleitschuh.gradle.ktlint")

    // 与 Initializr 生成的单工程种子保持一致的编译配置
    plugins.withId("org.jetbrains.kotlin.jvm") {
        extensions.configure<JavaPluginExtension>("java") {
            toolchain {
                languageVersion = JavaLanguageVersion.of(21)
            }
        }
        extensions.configure<KotlinJvmProjectExtension>("kotlin") {
            compilerOptions {
                freeCompilerArgs.addAll(
                    "-Xjsr305=strict",
                    "-Xannotation-default-target=param-property",
                    "-Xemit-jvm-type-annotations",
                )
            }
        }
    }

    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}
