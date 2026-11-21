package dev.yidafu.flowtrace.shared.persistence.statement

import org.jetbrains.exposed.v1.core.Column
import org.jetbrains.exposed.v1.core.IColumnType
import org.jetbrains.exposed.v1.core.QueryBuilder
import org.jetbrains.exposed.v1.core.Table
import org.jetbrains.exposed.v1.core.Transaction
import org.jetbrains.exposed.v1.core.statements.BatchInsertStatement
import org.jetbrains.exposed.v1.jdbc.statements.BatchInsertBlockingExecutable

/**
 * 「单条多行」批量插入语句：N 行合并为一条
 * `INSERT INTO t (cols) VALUES (...),(...),… ON CONFLICT (<conflictKeys>) DO NOTHING`。
 *
 * 为什么自定义 [BatchInsertStatement] 而不用 Exposed 的 `batchInsert(ignore = true)`：
 * 后者是 JDBC batch（N 行 = N 条同构单行 INSERT），pgjdbc 的 `reWriteBatchedInserts`
 * 不会改写带 `ON CONFLICT` 的语句，等于 N 次网络往返；多行单语句实测比逐行快约 10 倍
 * （docs/reports/2026-11-21-4c8g-verification.md §8，`TracePointRepository.batchInsertIgnore`
 * 的手写 SQL 同款结论）。
 *
 * 实现要点（Exposed 1.0.0-beta-4）：
 * - SQL 由 [prepareSQL] 生成：父类只产单行 VALUES 元组，这里按批行数复制元组；表名/列名/
 *   冲突目标走 `transaction.identity`（带必要引号），VALUES 元组经 [QueryBuilder.registerArgument]
 *   参数化绑定（类型取各列 columnType），替代手写 `?` 占位与 `exec(sql, args)`；
 * - 绑定参数由 [arguments] 扁平化为**单条序列**（父类是每行一组）：执行侧
 *   （[BatchInsertBlockingExecutable]，`isAlwaysBatch = true`）因此会把全部行参数一次
 *   `fillParameters` 后仅 `addBatch()` 一次 → `executeBatch()` 只执行这一条多行语句，
 *   单次往返；
 * - 冲突动作固定为 `DO NOTHING`（幂等写入语义：并发重放丢重复行比覆盖更安全）。
 *
 * 用法：**`addBatch()` 必须先于每行的列赋值**——Exposed 的 `addBatch()` 会把当前 values
 * 快照进 `data` 再换出新行，先赋值后 addBatch 会丢掉第一行（与 Exposed 内部
 * `Table.batchInsert` 的调用序一致）：
 *
 * ```kotlin
 * val stmt = MultiRowInsertIgnoreStatement(table, listOf(table.id))
 * rows.forEach { row ->
 *     stmt.addBatch()
 *     stmt[table.foo] = row.foo
 * }
 * BatchInsertBlockingExecutable(stmt).execute(TransactionManager.current())
 * ```
 *
 * 约束：每行必须赋同样的列集（或整批统一省略带 DB 默认值的列），否则占位符与绑定值
 * 无法对齐，[arguments] 会直接抛错；带 client default（`default()`）的列在批量场景下
 * 会被父类的 `BatchDataInconsistentException` 校验拒绝。
 */
internal class MultiRowInsertIgnoreStatement(
    table: Table,
    private val conflictKeys: List<Column<*>>,
) : BatchInsertStatement(table, ignore = false, shouldReturnGeneratedValues = false) {
    /**
     * 扁平化绑定参数：把父类的「每行一组」折叠为「单条全量」，配合执行侧单次
     * `fillParameters + addBatch` 完成 N 行一次绑定。父类按行过滤 `DefaultValueMarker`
     * （DB 默认值列），各行省略列不一致会破坏占位符对齐，这里直接拒绝。
     */
    override fun arguments(): List<List<Pair<IColumnType<*>, Any?>>> {
        val rows = super.arguments().map { it.toList() }
        if (rows.size <= 1) return rows
        val width = rows.first().size
        require(rows.all { it.size == width }) {
            "单条多行 INSERT 要求每行绑定参数一致（DB 默认值列的省略必须整批统一），" +
                "否则占位符与参数无法对齐：${rows.map { it.size }}"
        }
        return listOf(rows.flatten())
    }

    /**
     * 生成多行 VALUES + `ON CONFLICT (...) DO NOTHING`。列集合取自父类 arguments
     * （全部批行的并集、按首次出现保序），与 [arguments] 同源，保证占位符数与绑定参数数一致。
     */
    override fun prepareSQL(
        transaction: Transaction,
        prepared: Boolean,
    ): String {
        val rows = arguments ?: error("No data provided for inserting into ${table.tableName}")
        require(rows.isNotEmpty()) { "No data provided for inserting into ${table.tableName}" }
        val builder = QueryBuilder(prepared)
        with(builder) {
            rows.appendTo(separator = ",") { row ->
                row.appendTo(separator = ",", prefix = "(", postfix = ")") { (column, value) ->
                    registerArgument(column, value)
                }
            }
        }
        val insert =
            transaction.db.dialect.functionProvider
                .insert(isIgnore, table, rows.first().map { it.first }, "VALUES $builder", transaction)
        val conflictTarget = conflictKeys.joinToString(", ") { transaction.identity(it) }
        return "$insert ON CONFLICT ($conflictTarget) DO NOTHING"
    }
}
