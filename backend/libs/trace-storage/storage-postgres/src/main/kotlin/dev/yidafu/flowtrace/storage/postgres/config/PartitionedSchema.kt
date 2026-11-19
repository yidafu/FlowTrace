package dev.yidafu.flowtrace.storage.postgres.config

import dev.yidafu.flowtrace.storage.postgres.table.AuditLogTable
import dev.yidafu.flowtrace.storage.postgres.table.TraceBatchTable
import dev.yidafu.flowtrace.storage.postgres.table.TracePointTable
import dev.yidafu.flowtrace.storage.postgres.table.TraceSegmentTable
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.jdbc.SchemaUtils
import org.jetbrains.exposed.v1.jdbc.transactions.TransactionManager
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/** 分区粒度（03 §2~§5）。 */
internal enum class PartitionGranularity {
    /** 按天分区：trace_point（ts）/ trace_batch（received_at）。 */
    DAY {
        override fun periodStart(date: LocalDate): LocalDate = date

        override fun plusPeriods(
            date: LocalDate,
            periods: Long,
        ): LocalDate = date.plusDays(periods)

        override fun suffix(date: LocalDate): String = date.format(DateTimeFormatter.BASIC_ISO_DATE)
    },

    /** 按月分区：trace_segment（start_ts）/ audit_log（created_at）。 */
    MONTH {
        override fun periodStart(date: LocalDate): LocalDate = date.withDayOfMonth(1)

        override fun plusPeriods(
            date: LocalDate,
            periods: Long,
        ): LocalDate = date.plusMonths(periods)

        override fun suffix(date: LocalDate): String = date.format(MONTH_SUFFIX)
    },

    ;

    /** 分区起点：按天即当天，按月即当月 1 日。 */
    abstract fun periodStart(date: LocalDate): LocalDate

    abstract fun plusPeriods(
        date: LocalDate,
        periods: Long,
    ): LocalDate

    /** 分区名后缀：`yyyyMMdd` / `yyyyMM`（03 §7 的 `trace_point_p20261118` 命名）。 */
    abstract fun suffix(date: LocalDate): String

    private companion object {
        val MONTH_SUFFIX: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMM")
    }
}

/**
 * 一张分区主表的建表描述。
 *
 * @param table 列定义事实源（Exposed `Table` 对象，读写映射复用）
 * @param partitionColumn 分区键列名，必须是主键成员（PG 对分区表强制要求）
 * @param granularity 分区粒度
 * @param preCreatePeriods 预建周期数（含当前周期）：按天 4 = 今天起未来 3 天（03 §7）
 */
internal class PartitionedTable(
    val table: Table,
    val partitionColumn: String,
    val granularity: PartitionGranularity,
    val preCreatePeriods: Int,
)

/**
 * 分区主表与分区子表的运行时 DDL（03 §7、决策 D9）。
 *
 * 为什么不能交给 `SchemaUtils.create`：Exposed 生成的是无分区普通表，建成后再也拿不到
 * `DROP PARTITION` 的低成本过期删除与分区裁剪（03 §6 容量账）。所以这里复用
 * `Table.createStatement()` 生成列定义（保证与 Table 对象不漂移），只在末尾补
 * `PARTITION BY RANGE (...)`；索引仍由 Table 对象声明的 `indices` 生成，建在主表上
 * 由 PG 自动下建到各子表。
 *
 * 只负责**首次建表 + 预建分区**：已有主表的结构演进仍走 exposed-migration（决策 D16），
 * 因此主表已存在时直接跳过，不做任何 ALTER。
 */
internal object PartitionedSchema {
    private val log = LoggerFactory.getLogger(javaClass)

    private val tables =
        listOf(
            // 按天 + 未来 3 天（03 §7.1）
            PartitionedTable(TracePointTable, "ts", PartitionGranularity.DAY, 4),
            PartitionedTable(TraceBatchTable, "received_at", PartitionGranularity.DAY, 4),
            // 按月 + 预建未来 2 个月（03 §7.1）
            PartitionedTable(TraceSegmentTable, "start_ts", PartitionGranularity.MONTH, 3),
            PartitionedTable(AuditLogTable, "created_at", PartitionGranularity.MONTH, 3),
        )

    /**
     * 补建缺失的分区主表、当期与后续周期分区、以及 `*_default` 兜底分区（幂等）。
     *
     * **必须在 `transaction {}` 内调用**：`Table.createStatement()` 依赖当前事务的方言与标识符管理器。
     */
    fun createAll(today: LocalDate = LocalDate.now()) {
        tables.forEach { spec ->
            val created = createParent(spec)
            createPartitions(spec, today)
            createDefaultPartition(spec)
            if (created) {
                log.info(
                    "分区主表已创建 table={} partitionBy={} granularity={}",
                    spec.table.tableName,
                    spec.partitionColumn,
                    spec.granularity,
                )
            }
        }
    }

    /** @return true 表示本次真的建了主表 */
    private fun createParent(spec: PartitionedTable): Boolean {
        val table = spec.table
        if (exists(table.tableName)) return false

        val createTable = table.createStatement().single()
        require(createTable.startsWith("CREATE TABLE")) { "无法识别的建表语句：$createTable" }
        exec("${createTable.trimEnd()}\n  PARTITION BY RANGE (${spec.partitionColumn})")
        // Exposed 生成的 CREATE INDEX 不带 IF NOT EXISTS；Exposed 事务遇错会重试整段，
        // 补成幂等语句才能让重试不撞已建好的索引。
        table.indices.forEach { index ->
            SchemaUtils.createIndex(index).map(::idempotentIndexDdl).forEach(::exec)
        }
        return true
    }

    private fun idempotentIndexDdl(statement: String): String =
        when {
            statement.startsWith("CREATE UNIQUE INDEX ") ->
                statement.replaceFirst("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ")

            statement.startsWith("CREATE INDEX ") ->
                statement.replaceFirst("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ")

            else -> statement
        }

    /**
     * 主表是否存在。
     *
     * 不能用 Exposed 的 `Table.exists()`：它走 information_schema 判定，对**分区主表**
     * （`pg_class.relkind = 'p'`）会返回 false，导致第二次启动重复执行建索引而报
     * `relation "trace_point_user_ts_idx" already exists`。`to_regclass` 对普通表、
     * 分区主表、视图一律按名字解析，语义正好是「这个名字有没有对象占用」。
     */
    private fun exists(tableName: String): Boolean =
        TransactionManager.current().exec("SELECT to_regclass('$tableName')") { rs ->
            rs.next() && rs.getString(1) != null
        } == true

    private fun createPartitions(
        spec: PartitionedTable,
        today: LocalDate,
    ) {
        val first = spec.granularity.periodStart(today)
        repeat(spec.preCreatePeriods) { offset ->
            val from = spec.granularity.plusPeriods(first, offset.toLong())
            val to = spec.granularity.plusPeriods(from, 1)
            createPartition(spec, spec.granularity.suffix(from), from, to)
        }
    }

    /**
     * 兜底分区（03 §7.2）：捕获越界时间戳（端侧时钟漂移）而不是让整批写入失败。
     *
     * 副作用：DEFAULT 分区里已有某范围的脏数据时，再补建该范围的具体分区会被 PG 拒绝
     * （`updated partition constraint would be violated`）——按 03 §7.2，正确处置是先校准
     * 并搬运 default 里的数据，不是删分区。
     */
    private fun createDefaultPartition(spec: PartitionedTable) {
        exec(
            "CREATE TABLE IF NOT EXISTS ${spec.table.tableName}_default " +
                "PARTITION OF ${spec.table.tableName} DEFAULT",
        )
    }

    private fun createPartition(
        spec: PartitionedTable,
        suffix: String,
        from: LocalDate,
        to: LocalDate,
    ) {
        exec(
            "CREATE TABLE IF NOT EXISTS ${spec.table.tableName}_p$suffix " +
                "PARTITION OF ${spec.table.tableName} " +
                "FOR VALUES FROM ('${boundary(from)}') TO ('${boundary(to)}')",
        )
    }

    /**
     * 分区边界字面量。
     *
     * 列类型是 `timestamp`（无时区，与 device / dirty_day 一致），Exposed 写入 `Instant` 时按
     * JVM 默认时区折算成墙上时间，所以边界也用同一时区的零点，保证「按天」与写入值对齐。
     */
    private fun boundary(date: LocalDate): String = "$date 00:00:00"

    private fun exec(sql: String) {
        TransactionManager.current().exec(sql)
    }
}
