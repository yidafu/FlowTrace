import org.gradle.api.artifacts.VersionCatalogsExtension

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.protobuf)
}

// 通过 VersionCatalogsExtension 取版本：类型安全访问器（libs.versions.protobuf.get()）
// 在 Kotlin DSL 脚本里与 protobuf 插件的访问器生成冲突，编译不过
val protobufVersion by lazy {
    extensions
        .getByType(VersionCatalogsExtension::class.java)
        .named("libs")
        .findVersion("protobuf")
        .get()
}

dependencies {
    api(project(":libs:trace-model"))
    // Protobuf 运行时（protobuf-kotlin 传递引入 protobuf-java）
    api(libs.protobuf.kotlin)
}

// 协议事实源在仓库根目录 proto/（SDK 与服务端共用），直接指向该目录，不做文件复制
sourceSets {
    main {
        proto {
            srcDir("../../../proto")
        }
    }
}

protobuf {
    protoc {
        // 版本需与 protobuf-kotlin 运行时一致（见 gradle/libs.versions.toml）
        artifact = "com.google.protobuf:protoc:$protobufVersion"
    }
    generateProtoTasks {
        all().forEach {
            // 默认生成 Java（Kotlin DSL 以其为基础），追加 Kotlin DSL 生成
            it.builtins {
                create("kotlin")
            }
        }
    }
}

// 生成代码（build/generated/**）不参与 ktlint
// ktlint { filter {} } 的路径模式不生效，直接在任务级用路径判断排除
tasks.withType<org.jlleitschuh.gradle.ktlint.tasks.BaseKtLintCheckTask>().configureEach {
    exclude { it.file.invariantSeparatorsPath.contains("/generated/") }
}
