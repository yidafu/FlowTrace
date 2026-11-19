plugins {
    alias(libs.plugins.kotlin.jvm)
}

// 领域模型：轨迹点/轨迹段/上报批次等纯数据结构。
// 唯一外部依赖是 JTS：只用于抽稀（Douglas-Peucker，06 §1.1 / 决策 D7），
// 其余几何计算（haversine、局部投影、bbox、WKT 拼装）全部自实现，不依赖数据库扩展。
dependencies {
    api(libs.jts.core)
    testImplementation(kotlin("test"))
}
