package zed.rainxch.core.domain.network

import kotlinx.coroutines.flow.Flow
import zed.rainxch.core.domain.model.installation.DownloadProgress

/**
 * The identity of the asset a download must produce (PR-1 D-2).
 *
 * The caller (the orchestrator, which holds the [zed.rainxch.core.domain.system.DownloadSpec]) passes
 * this so a downloader can prove that a partial left on disk still belongs to *this* asset before
 * appending to it. A rolling tag (`nightly`) keeps the asset name while its bytes change; resuming
 * across such a change writes a corrupt APK.
 *
 * For GitHub, `digest` is authoritative (content), `assetId` identifies the immutable upload, and
 * `size` is the last resort. A host that offers none of these simply yields a weak [AssetIdentity].
 *
 * Passing `null` for the whole identity is meaningful — it says "the caller cannot describe the
 * target asset", and a downloader must then refuse to resume anything rather than guess from the
 * file name (PR-1 rule 1d).
 */
data class AssetIdentity(
    val assetId: Long = 0L,
    val digest: String? = null,
    val size: Long = -1L,
)

interface Downloader {
    fun download(
        url: String,
        suggestedFileName: String? = null,
        bypassMirror: Boolean = false,
        identity: AssetIdentity? = null,
    ): Flow<DownloadProgress>

    suspend fun saveToFile(
        url: String,
        suggestedFileName: String? = null,
    ): String

    suspend fun getDownloadedFilePath(fileName: String): String?

    suspend fun cancelDownload(fileName: String): Boolean

    /**
     * D-8 "delete": stop any transfer for [fileName] and remove its partial + sidecar from disk.
     *
     * This is the *only* downloader operation allowed to delete resumable bytes on purpose; a pause
     * ([cancelDownload]) must never call it. Deletion has to live here, not in the orchestrator,
     * because only the downloader knows the naming scheme and holds the per-name write lock.
     *
     * Returns true when at least one of the two files was removed.
     */
    suspend fun discardPartial(fileName: String): Boolean

    /**
     * D-7: sweep leftover partials (`.part` / `.part.meta`) that nobody claims any more, returning
     * how many files were removed.
     *
     * Ownership is **claim-based, not time-based**: [claimedNames] is the set of asset file names
     * that currently have an in-memory download entry. A partial whose name is in the set is a live
     * download and is kept; a partial whose name is not in the set can never be resumed (the entry
     * that owned it is gone for good) and is deleted. There is deliberately no retention window.
     *
     * Must never throw in a way that reaches a download: callers run it off the UI path and treat a
     * failure as non-fatal. Finished `.apk`s are never touched — only the partial pairs are.
     */
    suspend fun reclaimOrphanedPartials(claimedNames: Set<String>): Int
}
