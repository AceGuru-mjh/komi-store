package zed.rainxch.core.data.download

import zed.rainxch.core.domain.network.AssetIdentity

/**
 * Pure decision layer for resumable downloads (PR-1).
 *
 * Everything in this file is deliberately free of file-system and HTTP types: the three
 * questions below are the whole contract, and each one is a total function so it can be
 * pinned by a unit test without a device, a server, or a network.
 *
 *  1. [PartialNaming]      — where the partial and its sidecar live, deterministically.
 *  2. [PartialOwnership]   — is the partial on disk still *this* asset? (the ownership check)
 *  3. [RangeDecision]      — what did the server do with our `Range` header?
 *  4. [OrphanVerdict]      — may this leftover be deleted, or is it a live/parked partial?
 *
 * The platform downloaders ([zed.rainxch.core.data.services.AndroidDownloader] and its desktop
 * twin) keep only the IO and delegate every branch to here, so the two cannot drift apart in
 * behaviour the way they did with the duplicated `tempFile.delete()` bug.
 *
 * Invariant I7 lives here: nothing in this file deletes a partial because a coroutine was
 * cancelled. Deletion is only ever produced by [PartialOwnership.decideReuse] (ownership failed),
 * [RangeDecision.RESTART] (416), or an explicit user discard.
 */

// ---------------------------------------------------------------------------------------------
// 1. Naming
// ---------------------------------------------------------------------------------------------

/**
 * Deterministic names for a download's partial and its sidecar.
 *
 * The old scheme was `"$safeName.part-$downloadId"` with a fresh random `downloadId` per attempt,
 * which made the partial of a killed download unreachable: the next attempt generated a new uuid,
 * looked at a different path, and started from zero. `safeName` (from
 * [zed.rainxch.core.domain.utils.AssetFileName.scoped]) is already deterministic — it depends only
 * on owner, repo and asset name — so dropping the uuid suffix is all that is needed.
 *
 * [isPartFile] also recognises the legacy random names so orphan collection can sweep them up
 * (design E11): they have no sidecar, so the "part without meta" rule disposes of them.
 */
object PartialNaming {

    const val PART_SUFFIX = ".part"
    const val META_SUFFIX = ".part.meta"

    private val LEGACY_PART = Regex("""\.part-[0-9A-Za-z-]+$""")

    fun part(safeName: String): String = "$safeName$PART_SUFFIX"

    fun meta(safeName: String): String = "$safeName$META_SUFFIX"

    /**
     * True for both the current `<safeName>.part` and the legacy `<safeName>.part-<uuid>`.
     *
     * The sidecar is checked first: `"x.apk.part.meta"` must not be mistaken for a partial, or
     * orphan collection would delete a live download's metadata.
     */
    fun isPartFile(fileName: String): Boolean =
        !isMetaFile(fileName) &&
            (fileName.endsWith(PART_SUFFIX) || LEGACY_PART.containsMatchIn(fileName))

    fun isMetaFile(fileName: String): Boolean = fileName.endsWith(META_SUFFIX)

    /**
     * True only for the legacy `<safeName>.part-<id>` shape: a partial written before sidecars
     * existed, which therefore has to be recognised by its name alone (E11).
     *
     * A canonical `<safeName>.part` is deliberately **not** included, and the difference is what
     * keeps finished downloads safe. I4 writes the sidecar before the first byte, so a lone
     * canonical `.part` is never something we left behind — while the name itself can belong to a
     * *finished* download whose asset is called `<safeName>.part`. Collection derives `<safeName>`
     * from the file name and then deletes `<safeName>.part`; for a finished file that is the file
     * itself, so offering it as a candidate deletes a completed download as though it were the
     * orphaned partial of a different asset.
     */
    fun isLegacyPartFile(fileName: String): Boolean =
        !isMetaFile(fileName) && LEGACY_PART.containsMatchIn(fileName)

    /** The asset file name a partial belongs to, or null if [fileName] is not a partial. */
    fun assetNameOfPart(fileName: String): String? {
        if (isMetaFile(fileName)) return null
        val base =
            if (fileName.endsWith(PART_SUFFIX)) {
                fileName.removeSuffix(PART_SUFFIX)
            } else {
                fileName.replace(LEGACY_PART, "")
            }
        return base.takeIf { it.isNotEmpty() && it != fileName }
    }

    /** The asset file name a sidecar belongs to, or null if [fileName] is not a sidecar. */
    fun assetNameOfMeta(fileName: String): String? =
        if (isMetaFile(fileName)) fileName.removeSuffix(META_SUFFIX) else null
}

// ---------------------------------------------------------------------------------------------
// 2. Ownership
// ---------------------------------------------------------------------------------------------

/**
 * What we record next to a partial so a later attempt can tell whether the bytes on disk are still
 * the bytes it wants.
 *
 * Why this is not optional: a rolling tag (`nightly`) keeps the asset *name* while its *content*
 * changes. Appending to a partial left over from the previous build would produce a corrupt APK
 * that passes neither a size nor a digest check but has already been written to disk. So the
 * sidecar is the price of resuming safely, not a debugging aid.
 */
data class PartialIdentity(
    val assetId: Long,
    val digest: String?,
    val size: Long,
) {
    fun serialize(): String =
        buildString {
            append("assetId=").append(assetId).append('\n')
            append("digest=").append(digest.orEmpty()).append('\n')
            append("size=").append(size).append('\n')
        }

    companion object {
        /**
         * Parses the `key=value` sidecar, or null when it cannot be trusted.
         *
         * Lenient about unknown keys and about ordering (so the format can grow), strict about the
         * two numeric fields: an unparsable sidecar means "we do not know what this partial is",
         * and the only safe answer to that is to discard it.
         */
        fun parse(text: String): PartialIdentity? {
            var assetId: Long? = null
            var size: Long? = null
            var digest: String? = null

            for (rawLine in text.lineSequence()) {
                val line = rawLine.trim()
                if (line.isEmpty()) continue
                val separator = line.indexOf('=')
                if (separator <= 0) continue
                val key = line.substring(0, separator).trim()
                val value = line.substring(separator + 1).trim()
                when (key) {
                    "assetId" -> assetId = value.toLongOrNull()
                    "size" -> size = value.toLongOrNull()
                    "digest" -> digest = value.takeIf { it.isNotEmpty() }
                }
            }

            val parsedId = assetId ?: return null
            val parsedSize = size ?: return null
            return PartialIdentity(assetId = parsedId, digest = digest, size = parsedSize)
        }
    }
}

enum class PartialReuse {
    /** The bytes on disk belong to this asset — append to them. */
    REUSE,

    /** They do not (or we cannot prove they do) — delete the partial and start over. */
    DISCARD,
}

object PartialOwnership {

    /**
     * Decides whether an on-disk partial may be resumed.
     *
     * Evidence strength follows the identity model of #934 — `digest` (content) beats `assetId`
     * (object) beats `size` (shape):
     *
     *  - both digests present  ⇒ compare digests. This is the only *content* proof, and it is the
     *    reason a `--clobber` re-upload (same bytes, new asset id) does not force a re-download.
     *  - else compare `assetId` when both sides have a usable one (> 0). An id identifies one
     *    immutable upload; a different id means different bytes.
     *  - else compare `size`. Weakest proof: a same-size different build slips through, which is
     *    why it is only reached when the host offers neither digest nor id.
     *
     * Any of the prerequisites failing (no partial, empty partial, no sidecar, unparsable
     * sidecar) is a DISCARD — [PartialReuse.DISCARD] never means "unknown, proceed anyway".
     */
    fun decideReuse(
        partExists: Boolean,
        partLength: Long,
        meta: PartialIdentity?,
        incoming: PartialIdentity,
    ): PartialReuse {
        if (!partExists || partLength <= 0L) return PartialReuse.DISCARD
        if (meta == null) return PartialReuse.DISCARD

        val onDiskDigest = meta.digest
        val wantedDigest = incoming.digest
        if (onDiskDigest != null && wantedDigest != null) {
            return if (onDiskDigest == wantedDigest) PartialReuse.REUSE else PartialReuse.DISCARD
        }

        if (meta.assetId > 0L && incoming.assetId > 0L) {
            return if (meta.assetId == incoming.assetId) PartialReuse.REUSE else PartialReuse.DISCARD
        }

        return if (meta.size == incoming.size) PartialReuse.REUSE else PartialReuse.DISCARD
    }

    /**
     * The downloader-facing overload: [incoming] is what the caller could tell us about the target
     * asset, and `null` means "nothing" — see PR-1 rule 1d.
     *
     * **`identity == null` must never resume.** With no identity there is no proof the bytes on disk
     * are still this asset, and appending to unverified bytes can produce a corrupt file. Failing
     * closed here costs at most one redundant download; failing open costs a broken APK.
     */
    fun decideReuse(
        partExists: Boolean,
        partLength: Long,
        meta: PartialIdentity?,
        incoming: AssetIdentity?,
    ): PartialReuse {
        if (incoming == null) return PartialReuse.DISCARD
        return decideReuse(
            partExists = partExists,
            partLength = partLength,
            meta = meta,
            incoming = PartialIdentity(
                assetId = incoming.assetId,
                digest = incoming.digest,
                size = incoming.size,
            ),
        )
    }
}

// ---------------------------------------------------------------------------------------------
// 3. Range response
// ---------------------------------------------------------------------------------------------

enum class RangeDecision {
    /** `206 Partial Content` — the server resumed where we asked; append. */
    APPEND,

    /** `200 OK` in reply to a `Range` request — the server ignored it; rewrite the partial. */
    TRUNCATE_REWRITE,

    /** `416 Range Not Satisfiable` — our partial is bigger than the asset; it changed. Start over. */
    RESTART,

    /** A 2xx we did not plan for. The caller must treat it as an error. */
    UNEXPECTED,
}

/**
 * A parsed `Content-Range` response header (RFC 7233 §4.2), e.g. `bytes 500-999/2000`.
 *
 * The unsatisfied-range form replaces the numeric span with `*`, so it reports only the entity's
 * length and carries no offset; [start] is null there. [start] is also null when the header is
 * absent or malformed. Callers must read a null [start] as "no proof", never as "matches".
 */
data class ContentRange(val start: Long?, val total: Long?) {
    companion object {
        /**
         * Parses a `bytes` range. Returns null when [value] is absent, is not a `bytes` range, or
         * yields neither an offset nor a length — so a non-null result always means "a real
         * `Content-Range` was read", and null always means "no proof". Never returns an object that
         * carries no information.
         */
        fun parse(value: String?): ContentRange? {
            val body = value?.trim()?.takeIf { it.isNotEmpty() } ?: return null
            if (!body.substringBefore(' ', "").trim().equals("bytes", ignoreCase = true)) return null
            val spec = body.substringAfter(' ', "").trim()
            if (spec.isEmpty()) return null
            val total = spec.substringAfterLast('/', "").trim().toLongOrNull()
            val range = spec.substringBefore('/').trim()
            val start =
                if (range.startsWith("*")) null
                else range.substringBefore('-').trim().toLongOrNull()
            if (start == null && total == null) return null
            return ContentRange(start = start, total = total)
        }
    }
}

object RangeRequest {

    /** No `Range` header when there is nothing to resume from. */
    fun headerValue(partLength: Long): String? =
        if (partLength > 0L) "bytes=$partLength-" else null

    /**
     * Maps the response status to what the writer must do.
     *
     * `200` on a *fresh* download is the normal success case, not a downgrade, so it is APPEND
     * (writing an empty partial is the same as writing from the start). On a *resumed* download a
     * `200` means the server sent the whole asset from byte zero, and appending that to a partial
     * would duplicate its prefix — so it must rewrite.
     *
     * [contentRange] is the parsed `Content-Range` of the response, or null when the response
     * carried none. It is a required argument so no call site can forget it.
     *
     * A `206` is only proof of a *correct* resume when its offset equals the offset we asked for.
     * RFC 7233 §4.1 requires every `206` to carry `Content-Range`, and §4.2's offset is the only
     * thing that says the server honoured our `Range` instead of quietly serving some other span.
     * A `206` we cannot verify — no header, malformed header, an unsatisfied range that names no
     * offset, or a start we did not ask for — must be RESTART: appending unverified bytes grafts
     * the wrong span onto the partial and yields a file that is silently corrupt while looking
     * complete. Re-downloading a partial is always cheaper than shipping a broken APK, and the
     * restart path degrades to a fresh full download (no `Range` header), so it cannot loop.
     */
    fun decide(
        statusCode: Int,
        partLength: Long,
        contentRange: ContentRange?,
    ): RangeDecision =
        when (statusCode) {
            206 -> if (contentRange?.start == partLength) RangeDecision.APPEND else RangeDecision.RESTART
            416 -> RangeDecision.RESTART
            200 -> if (partLength > 0L) RangeDecision.TRUNCATE_REWRITE else RangeDecision.APPEND
            else -> RangeDecision.UNEXPECTED
        }
}

// ---------------------------------------------------------------------------------------------
// 4. Orphan collection
// ---------------------------------------------------------------------------------------------

enum class OrphanVerdict {
    /** Someone still claims this name — leave the pair alone. */
    KEEP,

    /** A partial with no sidecar (including legacy random names): unprovable, delete the partial. */
    DROP_PART_ONLY,

    /** A sidecar with no partial: delete the metadata. */
    DROP_META_ONLY,
}

object OrphanPolicy {

    /**
     * Classifies one leftover *pair* (a partial, a sidecar, or both).
     *
     * A **complete pair** — the partial *and* its sidecar both present — is always kept, claimed or
     * not. It is the only shape that can be resumed, and whether its bytes are still the wanted
     * asset is decided at reuse time by [PartialOwnership.decideReuse] (the identity check), not
     * here. Deleting it on sight would throw away a resumable transfer on every restart.
     *
     * Only a **half pair** is stripped: the sidecar is the identity that makes the bytes
     * attributable, so a partial without it (including E11 legacy random names) can never be
     * resumed, and a sidecar without its partial points at nothing. Neither can ever complete, so
     * both are swept. No time window is involved.
     *
     * [isClaimed] still short-circuits first and keeps everything: cleanup must never race a
     * transfer that is currently writing, which is exactly what the "no sidecar" rule would do to a
     * partial whose sidecar has not been flushed yet.
     *
     * Only the `.part` / `.part.meta` pair is classified here. A parked finished `.apk` is owned by
     * the install DB (`pendingInstallFilePath`), not by this policy, and must never be swept.
     */
    fun classify(
        partExists: Boolean,
        metaExists: Boolean,
        isClaimed: Boolean,
    ): OrphanVerdict {
        if (!partExists && !metaExists) return OrphanVerdict.KEEP
        if (isClaimed) return OrphanVerdict.KEEP

        if (partExists && !metaExists) return OrphanVerdict.DROP_PART_ONLY
        if (!partExists) return OrphanVerdict.DROP_META_ONLY

        // A partial with its sidecar is a complete, resumable pair: keep it. A fresh process may
        // find it unclaimed, but the next download of the same asset resumes it (and a finished
        // file is verified and reused) rather than starting over. Only the half-pairs above are
        // swept, because they can never be attributed to an asset again.
        return OrphanVerdict.KEEP
    }
}

// ---------------------------------------------------------------------------------------------
// 5. Disposal and retry policy
// ---------------------------------------------------------------------------------------------

/**
 * Encodes design D-5 / invariant I7: which failures are allowed to *delete* a partial.
 *
 * The valuable half is the negative one — every interruption that is not listed here keeps the
 * bytes. In particular a cancellation (process death, read timeout, leaving a screen) must never
 * reach the positive branch, or "resume" would delete the very partial it exists to resume.
 */
object PartialDisposal {

    /**
     * The asset is gone: retrying — and resuming onto stale bytes — is pointless.
     *
     * 403 is deliberately NOT here. GitHub answers 403 for transient conditions far more often than
     * for a permanent refusal: primary and secondary rate limits both come back as 403 (with
     * `X-RateLimit-Remaining: 0`), so does an expired token. Treating it as terminal threw the
     * partial away and forced a full re-download on the most common failure a resumed transfer
     * meets — the exact opposite of what keeping the bytes is for. It now falls in with 408/429/5xx
     * and goes through the bounded retry.
     */
    private val TERMINAL_CLIENT_ERRORS = setOf(404, 410)

    fun shouldDiscardPartial(statusCode: Int?, explicitDiscard: Boolean): Boolean =
        explicitDiscard || (statusCode != null && statusCode in TERMINAL_CLIENT_ERRORS)
}

/**
 * D-5's bounded backoff: three attempts, waiting 1 s and then 4 s between them.
 *
 * Three attempts means two delays, so the retry loop only ever reaches the first two entries of
 * the table. The third is kept as the ceiling [backoffMillis] clamps to, so a mis-counted attempt
 * degrades to a long wait rather than an index error — it is not a third delay.
 */
object PartialRetry {

    const val MAX_ATTEMPTS = 3

    private val BACKOFF_MILLIS = longArrayOf(1_000L, 4_000L, 16_000L)

    /** True while another attempt is still allowed, given [attemptsMade] failures so far. */
    fun hasAttemptsLeft(attemptsMade: Int): Boolean = attemptsMade < MAX_ATTEMPTS

    /** Delay before the attempt that follows [attemptsMade] failures. */
    fun backoffMillis(attemptsMade: Int): Long =
        BACKOFF_MILLIS[attemptsMade.coerceIn(0, BACKOFF_MILLIS.lastIndex)]
}

// ---------------------------------------------------------------------------------------------
// 6. Finished-file reuse
// ---------------------------------------------------------------------------------------------

/**
 * Whether a finished file already on disk can stand in for the transfer that has not run yet.
 *
 * This is the decision, not the act: it answers "is it worth hashing this file", and the caller
 * hashes only when it says yes. Hashing a candidate is ~tens of MB, so it is deliberately behind a
 * cheap gate — but the gate must never pass something it cannot prove.
 *
 * Fail-closed in every direction: no finished file, no identity, no digest, or a size that
 * disagrees all answer `null`, which sends the caller down the ordinary transfer path. A file we
 * cannot *prove* is the wanted bytes is never reused.
 */
object FinishedFileReuse {

    /**
     * The digest the finished file must match, or `null` when it is not a candidate.
     *
     * A non-positive [expectedSize] means "size unknown" and skips the length check; it never means
     * "empty file", which is why it is a comparison against the declared size rather than
     * `length > 0`.
     */
    fun digestToVerify(
        destinationExists: Boolean,
        destinationLength: Long,
        expectedDigest: String?,
        expectedSize: Long,
    ): String? {
        if (!destinationExists) return null
        if (expectedDigest.isNullOrBlank()) return null
        if (expectedSize > 0L && destinationLength != expectedSize) return null
        return expectedDigest
    }
}
