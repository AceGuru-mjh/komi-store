package zed.rainxch.core.domain.system

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.StateFlow

interface DownloadOrchestrator {

    val downloads: StateFlow<Map<String, OrchestratedDownload>>

    fun observe(packageName: String): Flow<OrchestratedDownload?>

    suspend fun enqueue(spec: DownloadSpec): String

    suspend fun downgradeToDeferred(packageName: String)

    suspend fun cancel(packageName: String)

    /**
     * D-8 "delete": stop the download *and* clean up — the partial, its sidecar and the list entry.
     *
     * This is the counterpart of [cancel], which is "pause" and deliberately keeps the partial so a
     * later [enqueue] can resume. The distinction is the user's explicit intent, never the exception
     * type: a pause, a process kill and a read timeout all look alike to the coroutine machinery.
     */
    suspend fun discard(packageName: String)

    suspend fun installPending(packageName: String): InstallOutcome?

    fun dismiss(packageName: String)
}
