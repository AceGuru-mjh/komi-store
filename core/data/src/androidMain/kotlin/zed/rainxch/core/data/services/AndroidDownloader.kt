package zed.rainxch.core.data.services

import co.touchlab.kermit.Logger
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOn
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import okhttp3.Call
import okhttp3.Credentials
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import zed.rainxch.core.data.data_source.TokenStore
import zed.rainxch.core.data.download.FinishedFileReuse
import zed.rainxch.core.data.download.OrphanPolicy
import zed.rainxch.core.data.download.OrphanVerdict
import zed.rainxch.core.data.download.PartialDisposal
import zed.rainxch.core.data.download.PartialIdentity
import zed.rainxch.core.data.download.PartialNaming
import zed.rainxch.core.data.download.PartialOwnership
import zed.rainxch.core.data.download.PartialReuse
import zed.rainxch.core.data.download.PartialRetry
import zed.rainxch.core.data.download.ContentRange
import zed.rainxch.core.data.download.RangeDecision
import zed.rainxch.core.data.download.RangeRequest
import zed.rainxch.core.data.network.GithubAssetAuth
import zed.rainxch.core.data.network.ProxyManager
import zed.rainxch.core.data.network.resolveAndroidSystemProxy
import zed.rainxch.core.domain.model.installation.DownloadProgress
import zed.rainxch.core.domain.model.settings.ProxyConfig
import zed.rainxch.core.domain.model.settings.ProxyScope
import zed.rainxch.core.domain.network.AssetIdentity
import zed.rainxch.core.domain.network.DigestVerifier
import zed.rainxch.core.domain.network.Downloader
import java.io.File
import java.io.FileOutputStream
import java.net.Authenticator
import java.net.InetSocketAddress
import java.net.PasswordAuthentication
import java.net.Proxy
import java.nio.file.AtomicMoveNotSupportedException
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit

class AndroidDownloader(
    private val files: FileLocationsProvider,
    private val tokenStore: TokenStore,
    private val digestVerifier: DigestVerifier,
) : Downloader {
    private val activeDownloads = ConcurrentHashMap<String, Call>()
    private val idsByName = ConcurrentHashMap<String, MutableSet<String>>()

    /**
     * Download ids whose cancel arrived before their `Call` existed.
     *
     * `cancelDownload` can only act on a registered `Call`, and a `Call` is not created until the
     * transfer reaches the network — after the ownership check and, when a previous attempt already
     * produced the destination, a full-file digest. A cancel landing in that window used to find
     * nothing in [activeDownloads], return false, and let the transfer run to completion: the user
     * asked it to stop and nothing stopped. Recording the request closes the window because the
     * transfer now consumes it immediately after registering the `Call`.
     */
    private val cancelRequested = ConcurrentHashMap.newKeySet<String>()

    /**
     * D-9: with a deterministic partial name, two packages can map to the same `safeName`. This
     * serialises writers by name so a second transfer can never truncate (or interleave with) the
     * partial a first one is still appending to.
     *
     * Entries are intentionally never evicted: the key space is "asset file names ever downloaded",
     * and a `Mutex` is tiny compared to the race a remove/recreate would reopen.
     */
    private val nameLocks = ConcurrentHashMap<String, Mutex>()

    private fun buildClient(): OkHttpClient {
        Authenticator.setDefault(null)

        return OkHttpClient
            .Builder()
            .connectTimeout(30, TimeUnit.SECONDS)
            .readTimeout(60, TimeUnit.SECONDS)
            .writeTimeout(60, TimeUnit.SECONDS)
            .apply {
                when (val config = ProxyManager.currentConfig(ProxyScope.DOWNLOAD)) {
                    is ProxyConfig.None -> {
                        proxy(Proxy.NO_PROXY)
                    }

                    is ProxyConfig.System -> {

                        proxy(resolveAndroidSystemProxy())
                    }

                    is ProxyConfig.Http -> {
                        proxy(Proxy(Proxy.Type.HTTP, InetSocketAddress(config.host, config.port)))
                        if (config.username != null && config.password != null) {
                            proxyAuthenticator { _, response ->
                                response.request
                                    .newBuilder()
                                    .header(
                                        "Proxy-Authorization",
                                        Credentials.basic(config.username!!, config.password!!),
                                    ).build()
                            }
                        }
                    }

                    is ProxyConfig.Socks -> {
                        proxy(Proxy(Proxy.Type.SOCKS, InetSocketAddress(config.host, config.port)))
                        if (config.username != null && config.password != null) {
                            Authenticator.setDefault(
                                object : Authenticator() {
                                    override fun getPasswordAuthentication() =
                                        PasswordAuthentication(
                                            config.username,
                                            config.password!!.toCharArray(),
                                        )
                                },
                            )
                        }
                    }
                }
            }.build()
    }

    override fun download(
        url: String,
        suggestedFileName: String?,
        bypassMirror: Boolean,
        identity: AssetIdentity?,
    ): Flow<DownloadProgress> =

        flow {
            val client = buildClient()

            val dirPath = files.appDownloadsDir()
            val dir = File(dirPath)
            if (!dir.exists()) dir.mkdirs()

            val rawName =
                suggestedFileName?.takeIf { it.isNotBlank() }
                    ?: url
                        .substringAfterLast('/')
                        .substringBefore('?')
                        .substringBefore('#')
                        .ifBlank { "asset-${UUID.randomUUID()}.apk" }
            val safeName = rawName.substringAfterLast('/').substringAfterLast('\\')
            require(safeName.isNotBlank() && safeName != "." && safeName != "..") {
                "Invalid file name: $rawName"
            }

            val downloadId = UUID.randomUUID().toString()

            val destination = File(dir, safeName)
            val partFile = File(dir, PartialNaming.part(safeName))
            val metaFile = File(dir, PartialNaming.meta(safeName))

            Logger.d { "Starting download: $url (id=$downloadId, partial=${partFile.name})" }

            val lock = nameLocks.computeIfAbsent(safeName) { Mutex() }
            lock.withLock {
                idsByName.computeIfAbsent(safeName) { ConcurrentHashMap.newKeySet() }.add(downloadId)
                try {
                    var attemptsMade = 0
                    var restartsMade = 0
                    while (true) {
                        try {
                            downloadAttempt(
                                client = client,
                                url = url,
                                partFile = partFile,
                                metaFile = metaFile,
                                destination = destination,
                                downloadId = downloadId,
                                identity = identity,
                                emit = { emit(it) },
                            )
                            break
                        } catch (e: kotlin.coroutines.cancellation.CancellationException) {
                            // D-8 (option A) / I7: an interruption keeps the partial so a later
                            // attempt resumes. Deletion is never driven by cancellation.
                            throw e
                        } catch (e: PartialRestart) {
                            if (restartsMade >= MAX_RESTARTS) {
                                throw kotlinx.io.IOException(
                                    "The bytes on disk could not be resumed for $safeName",
                                )
                            }
                            restartsMade++
                            Logger.d {
                                "The partial cannot be resumed (server refused the range, or served a span we did not ask for); restarting from zero"
                            }
                        } catch (e: PartialAborted) {
                            // The Call was cancelled on purpose: stop, do not retry.
                            throw e.cause ?: e
                        } catch (e: PartialTerminal) {
                            throw e
                        } catch (e: Exception) {
                            // A cancelled coroutine can surface as a plain IOException once the
                            // Call is cancelled; do not turn that into a retry.
                            coroutineContext.ensureActive()
                            if (!PartialRetry.hasAttemptsLeft(attemptsMade + 1)) {
                                Logger.e(e) {
                                    "Download failed after ${PartialRetry.MAX_ATTEMPTS} attempts"
                                }
                                throw e
                            }
                            val backoff = PartialRetry.backoffMillis(attemptsMade)
                            Logger.w(e) {
                                "Download attempt ${attemptsMade + 1} failed; retrying in ${backoff}ms"
                            }
                            delay(backoff)
                            attemptsMade++
                        }
                    }
                } finally {
                    activeDownloads.remove(downloadId)
                    cancelRequested.remove(downloadId)
                    idsByName.computeIfPresent(safeName) { _, set ->
                        set.remove(downloadId)
                        if (set.isEmpty()) null else set
                    }
                }
            }
        }.flowOn(Dispatchers.IO)

    /**
     * One network attempt for [partFile]: ownership check → `Range` → response branch → write.
     *
     * The partial and its sidecar are the only state; every exit path either leaves a resumable
     * (partial + consistent sidecar) pair or removes both (I4).
     */
    private suspend fun downloadAttempt(
        client: OkHttpClient,
        url: String,
        partFile: File,
        metaFile: File,
        destination: File,
        downloadId: String,
        identity: AssetIdentity?,
        emit: suspend (DownloadProgress) -> Unit,
    ) {
        val stored = readIdentity(metaFile)
        // Ownership is proven against the caller-supplied asset identity, not against the sidecar
        // itself. A `null` identity means we cannot verify anything, and decideReuse then fails
        // closed: the partial is discarded and the transfer starts from zero (PR-1 rule 1d).
        val reusable =
            PartialOwnership.decideReuse(
                partExists = partFile.exists(),
                partLength = if (partFile.exists()) partFile.length() else 0L,
                meta = stored,
                incoming = identity,
            ) == PartialReuse.REUSE

        if (!reusable) {
            partFile.delete()
            metaFile.delete()
            // The delete can fail while another handle holds the file (Windows refuses to unlink an
            // open file). Those bytes were just declared untrustworthy — an unparsable sidecar, no
            // identity, or an identity mismatch — so resuming them is precisely what must not
            // happen, and `resumeFrom` below is read from the file's survival, not from the
            // decision. Truncate instead of trusting the delete; if even that fails, abandon the
            // attempt rather than send a Range request for a prefix that was rejected. Failing
            // closed costs one re-download; failing open costs a corrupt APK.
            if (partFile.exists()) {
                val truncated =
                    runCatching { FileOutputStream(partFile, false).close() }
                        .isSuccess && partFile.length() == 0L
                if (!truncated) {
                    throw kotlinx.io.IOException(
                        "Cannot discard a partial that failed its ownership check: ${partFile.absolutePath}",
                    )
                }
            }
        }
        val resumeFrom = if (partFile.exists()) partFile.length() else 0L

        // T2: a previous attempt may already have produced the finished file. Recompute its digest
        // (no new sidecar state) and, on a match, reuse it instead of re-downloading. The check is
        // fail-closed: no identity/digest or a size mismatch means we cannot prove the file is the
        // wanted bytes, so it falls through to the normal transfer below.
        val expectedDigest =
            FinishedFileReuse.digestToVerify(
                destinationExists = destination.exists(),
                destinationLength = destination.length(),
                expectedDigest = identity?.digest,
                expectedSize = identity?.size ?: -1L,
            )
        if (expectedDigest != null &&
            digestVerifier.verify(destination.absolutePath, expectedDigest) == null
        ) {
            // A `.part` / `.part.meta` pair sitting next to a finished file is waste for this
            // asset; drop it now so orphan collection does not have to.
            partFile.delete()
            metaFile.delete()
            val length = destination.length()
            Logger.d { "Reusing completed file: ${destination.absolutePath} ($length bytes)" }
            emit(DownloadProgress(length, length, 100))
            return
        }

        val request = buildRequest(url, RangeRequest.headerValue(resumeFrom))
        val call = client.newCall(request)
        activeDownloads[downloadId] = call
        // Honour a cancel that arrived while this Call did not exist yet. Cancelling before
        // `execute()` makes OkHttp fail the call, and the catch below turns that into
        // PartialAborted, which stops the transfer without deleting the bytes.
        if (cancelRequested.remove(downloadId)) call.cancel()
        // Make a coroutine cancellation (user pause / scope teardown) interrupt the blocking read
        // instead of waiting out the 60 s socket timeout while still holding the name lock.
        val cancellationHandle =
            coroutineContext[kotlinx.coroutines.Job]?.invokeOnCompletion { cause ->
                if (cause is kotlin.coroutines.cancellation.CancellationException) call.cancel()
            }
        try {
            call.execute().use { response ->
                // Parsed once and used for both the resume decision and the total length: the
                // offset in this header is the only proof that the server honoured our `Range`.
                val contentRange = ContentRange.parse(response.header("Content-Range"))
                val decision = RangeRequest.decide(response.code, resumeFrom, contentRange)
                if (decision == RangeDecision.RESTART) {
                    partFile.delete()
                    metaFile.delete()
                    throw PartialRestart()
                }
                if (decision == RangeDecision.UNEXPECTED) {
                    if (PartialDisposal.shouldDiscardPartial(response.code, explicitDiscard = false)) {
                        partFile.delete()
                        metaFile.delete()
                        throw PartialTerminal("Unexpected code ${response.code}")
                    }
                    throw kotlinx.io.IOException("Unexpected code ${response.code}")
                }

                val total = totalBytes(response, resumeFrom, contentRange)
                // A 206 whose total disagrees with the sidecar means the asset was replaced under a
                // reused name (rolling tag): the bytes we hold belong to the old asset. Start over.
                if (decision == RangeDecision.APPEND &&
                    resumeFrom > 0L &&
                    stored != null &&
                    stored.size > 0L &&
                    total != null &&
                    stored.size != total
                ) {
                    partFile.delete()
                    metaFile.delete()
                    throw PartialRestart()
                }

                // I4: persist the sidecar before any byte, so a kill mid-transfer always leaves a
                // partial that a later attempt can attribute and resume.
                //
                // Record the caller's identity when we have one: it is what makes the *next*
                // attempt's digest/assetId comparison meaningful. Without it the sidecar would only
                // carry the shape (size), and a same-size re-upload would resume into garbage.
                // With a null identity we still write a sidecar (I4) but a later null-identity
                // attempt refuses to resume anyway.
                writeIdentity(
                    metaFile,
                    identity?.let {
                        PartialIdentity(
                            assetId = it.assetId,
                            digest = it.digest,
                            size = if (it.size > 0L) it.size else (total ?: resumeFrom),
                        )
                    } ?: PartialIdentity(assetId = 0L, digest = null, size = total ?: resumeFrom),
                )

                val append = decision == RangeDecision.APPEND
                if (!append) partFile.delete()
                val base = if (append) resumeFrom else 0L

                response.body.byteStream().use { input ->
                    FileOutputStream(partFile, append).use { output ->
                        val buffer = ByteArray(BUFFER_SIZE)
                        var downloaded = base
                        var bytesRead: Int
                        while (input.read(buffer).also { bytesRead = it } != -1) {
                            output.write(buffer, 0, bytesRead)
                            downloaded += bytesRead
                            val percent =
                                total?.takeIf { it > 0L }?.let {
                                    ((downloaded * 100L) / it).toInt()
                                }
                            emit(DownloadProgress(downloaded, total, percent))
                        }
                    }
                }

                val finalLength = partFile.length()
                if (finalLength <= 0L) {
                    throw IllegalStateException(
                        "Download produced empty file: ${partFile.absolutePath}",
                    )
                }
                if (total != null && total > 0L && finalLength != total) {
                    // A truncated stream is still a valid prefix: keep it and fail so the caller
                    // retries and resumes from the new length.
                    throw kotlinx.io.IOException(
                        "Incomplete download: got $finalLength of $total bytes",
                    )
                }

                moveAtomic(partFile, destination)
                metaFile.delete()

                Logger.d { "Download complete: ${destination.absolutePath}" }
                val finalPercent =
                    total?.takeIf { it > 0L }?.let { ((finalLength * 100L) / it).toInt() } ?: 100
                emit(DownloadProgress(finalLength, total, finalPercent))
            }
        } catch (e: kotlin.coroutines.cancellation.CancellationException) {
            throw e
        } catch (e: Exception) {
            // A direct `cancelDownload` cancels the Call but not this coroutine; without this guard
            // the retry loop would simply start the transfer again. The partial is kept either way
            // (I7): stopping a download must never delete bytes.
            if (call.isCanceled()) {
                coroutineContext.ensureActive()
                throw PartialAborted(e)
            }
            throw e
        } finally {
            cancellationHandle?.dispose()
        }
    }

    private suspend fun buildRequest(url: String, rangeHeader: String?): Request =
        Request
            .Builder()
            .url(url)
            .apply {
                val token = githubToken()
                if (token != null && GithubAssetAuth.isGithubHost(url)) {
                    header("Authorization", "Bearer $token")
                    if (GithubAssetAuth.isGithubApiHost(url)) {
                        header("Accept", "application/octet-stream")
                    }
                }
                if (rangeHeader != null) header("Range", rangeHeader)
            }.build()

    private fun readIdentity(metaFile: File): PartialIdentity? =
        try {
            if (metaFile.exists()) PartialIdentity.parse(metaFile.readText()) else null
        } catch (e: Exception) {
            Logger.w(e) { "Unreadable sidecar ${metaFile.name}; treating the partial as unattributable" }
            null
        }

    private fun writeIdentity(metaFile: File, identity: PartialIdentity) {
        try {
            metaFile.writeText(identity.serialize())
        } catch (e: Exception) {
            Logger.w(e) { "Failed to persist sidecar ${metaFile.name}" }
        }
    }

    /**
     * The full length of the asset, from `Content-Range` when resuming and `Content-Length`
     * otherwise, or null when the server does not tell us.
     */
    private fun totalBytes(response: Response, resumeFrom: Long, contentRange: ContentRange?): Long? {
        val declared = contentRange?.total
        if (declared != null && declared > 0L) return declared
        val contentLength = response.body.contentLength()
        if (contentLength > 0L) return if (response.code == 206) resumeFrom + contentLength else contentLength
        return null
    }

    private suspend fun githubToken(): String? =
        try {
            tokenStore.currentToken()?.accessToken?.trim()?.takeIf { it.isNotEmpty() }
        } catch (e: kotlin.coroutines.cancellation.CancellationException) {
            throw e
        } catch (_: Exception) {
            null
        }

    private fun moveAtomic(source: File, target: File) {
        try {
            Files.move(
                source.toPath(),
                target.toPath(),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.ATOMIC_MOVE,
            )
        } catch (_: AtomicMoveNotSupportedException) {

            Files.move(source.toPath(), target.toPath(), StandardCopyOption.REPLACE_EXISTING)
        }
    }

    /**
     * D-7: sweep partials that nobody claims any more (E11 legacy random names, orphan sidecars,
     * abandoned pairs). A partial is disposable exactly when its asset name has no in-memory
     * download entry — see [OrphanPolicy.classify]. There is no time window: the only two ways an
     * entry disappears are the user deleting it or the download finishing, and both cases already
     * mean the partial can never be resumed into anything.
     *
     * [claimedNames] is the set of asset file names (safeNames) that currently have an in-memory
     * download entry, computed by the caller. The downloader's own active-transfer registry is
     * unioned in so cleanup can never race a writer whose entry has not reached the caller yet.
     *
     * Run once at startup and once lazily before the first transfer; off the UI path either way.
     */
    override suspend fun reclaimOrphanedPartials(claimedNames: Set<String>): Int =
        withContext(Dispatchers.IO) {
            val dir = File(files.appDownloadsDir())
            if (!dir.exists()) return@withContext 0

            val names = dir.list()?.toList().orEmpty()
            val liveNames = claimedNames + idsByName.keys
            // Candidates reached *by name* are legacy partials only. A canonical `<asset>.part` is
            // excluded on purpose: I4 writes the sidecar before the first byte, so a lone canonical
            // `.part` is never something we left behind — while that exact name can belong to a
            // *finished* download whose asset is called `<asset>.part`. Deriving `<asset>` from it
            // and deleting `<asset>.part` would destroy that download. Canonical names are still
            // reached through their sidecar, which is the half that carries a pair into classify().
            val legacyPartNames = names.filter { PartialNaming.isLegacyPartFile(it) }
            val metaNames = names.filter { PartialNaming.isMetaFile(it) }
            val assetNames =
                (legacyPartNames.mapNotNull { PartialNaming.assetNameOfPart(it) } +
                    metaNames.mapNotNull { PartialNaming.assetNameOfMeta(it) }).toSet()

            var removed = 0
            for (assetName in assetNames) {
                val part = File(dir, PartialNaming.part(assetName))
                val meta = File(dir, PartialNaming.meta(assetName))

                val verdict =
                    OrphanPolicy.classify(
                        partExists = part.exists(),
                        metaExists = meta.exists(),
                        isClaimed = assetName in liveNames,
                    )
                removed += applyVerdict(verdict, part, meta)

                if (assetName in liveNames) continue
                // Legacy `<asset>.part-<uuid>` partials carry no sidecar; the "part without meta"
                // rule disposes of them (E11).
                legacyPartNames
                    .filter { PartialNaming.assetNameOfPart(it) == assetName && it != part.name }
                    .map { File(dir, it) }
                    .forEach { if (it.delete()) removed++ }
            }
            if (removed > 0) Logger.d { "Reclaimed $removed orphaned partial file(s)" }
            removed
        }

    private fun applyVerdict(verdict: OrphanVerdict, part: File, meta: File): Int {
        var removed = 0
        when (verdict) {
            OrphanVerdict.KEEP -> Unit
            OrphanVerdict.DROP_PART_ONLY -> if (part.delete()) removed++
            OrphanVerdict.DROP_META_ONLY -> if (meta.delete()) removed++
        }
        return removed
    }

    override suspend fun saveToFile(
        url: String,
        suggestedFileName: String?,
    ): String =
        withContext(Dispatchers.IO) {
            val rawName =
                suggestedFileName?.takeIf { it.isNotBlank() }
                    ?: url
                        .substringAfterLast('/')
                        .substringBefore('?')
                        .substringBefore('#')
                        .ifBlank { "asset-${UUID.randomUUID()}.apk" }
            val safeName = rawName.substringAfterLast('/').substringAfterLast('\\')
            require(safeName.isNotBlank() && safeName != "." && safeName != "..") {
                "Invalid file name: $rawName"
            }

            val file = File(files.appDownloadsDir(), safeName)

            // No pre-delete: the reuse check (T2) recognises an already-finished file and skips the
            // transfer; if a re-download is genuinely needed, the success path's `moveAtomic`
            // overwrites the old file.
            Logger.d { "saveToFile downloading file..." }
            download(url, suggestedFileName).collect { }

            file.absolutePath
        }

    override suspend fun getDownloadedFilePath(fileName: String): String? =
        withContext(Dispatchers.IO) {
            val file = File(files.appDownloadsDir(), fileName)

            if (file.exists() && file.length() > 0) {
                file.absolutePath
            } else {
                null
            }
        }

    override suspend fun cancelDownload(fileName: String): Boolean =
        withContext(Dispatchers.IO) {

            val ids = idsByName.remove(fileName)?.toList().orEmpty()
            // No live entry for this name: nothing to pause, and nothing worth remembering.
            if (ids.isEmpty()) return@withContext false

            for (id in ids) {
                // Record before looking for the Call: if the transfer has not created one yet there
                // is nothing to cancel here, but the request must not be dropped — the transfer
                // picks it up the moment it registers its Call.
                cancelRequested.add(id)
                activeDownloads.remove(id)?.takeIf { !it.isCanceled() }?.cancel()
            }

            // Every id came from a live entry, so each transfer was either cancelled on the spot or
            // will consume the recorded request as soon as it reaches the network.
            true
        }

    override suspend fun discardPartial(fileName: String): Boolean =
        withContext(Dispatchers.IO) {
            // Stop any in-flight transfer for this name before touching the files.
            cancelDownload(fileName)

            val dir = File(files.appDownloadsDir())
            // Serialise against an in-flight writer through the same per-name lock the transfer
            // holds: deleting a partial out from under an open handle silently fails on Windows and
            // races the move-to-destination elsewhere. `withLock` waits for the writer to unwind
            // (cancelled above) and release the handle first.
            val lock = nameLocks[fileName]
            if (lock == null) {
                deletePartialPair(dir, fileName)
            } else {
                lock.withLock { deletePartialPair(dir, fileName) }
            }
        }

    /**
     * The one place that maps a file name back to its partial + sidecar and removes both together.
     * This is D-8's "delete" — the partial and its metadata must never be left half-deleted (I4).
     */
    private fun deletePartialPair(dir: File, safeName: String): Boolean {
        val partRemoved = File(dir, PartialNaming.part(safeName)).delete()
        val metaRemoved = File(dir, PartialNaming.meta(safeName)).delete()
        return partRemoved || metaRemoved
    }

    private class PartialRestart : Exception()

    private class PartialTerminal(message: String) : Exception(message)

    private class PartialAborted(cause: Throwable) : Exception(cause)

    private companion object {
        private const val BUFFER_SIZE = 8 * 1024
        private const val MAX_RESTARTS = 2
    }
}
