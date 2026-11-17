plugins {
    alias(libs.plugins.kotlin.jvm)
}

// 领域模型：轨迹点/轨迹段/上报批次等纯数据结构，保持零外部依赖
dependencies {
    testImplementation(kotlin("test"))
}
