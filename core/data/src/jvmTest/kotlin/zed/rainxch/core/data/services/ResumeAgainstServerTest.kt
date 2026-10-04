package zed.rainxch.core.data.services

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.io.File
import java.net.InetSocketAddress
import java.nio.file.Files
import java.security.MessageDigest
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import zed.rainxch.core.data.download.PartialIdentity
import zed.rainxch.core.data.download.PartialNaming
import zed.rainxch.core.data.download.PartialRetry
import zed.rainxch.core.data.network.DesktopDigestVerifier
import zed.rainxch.core.domain.network.AssetIdentity
import zed.rainxch.core.domain.utils.AssetFileName

/**
 * Drives the real downloader against a real HTTP server that answers `Range` requests itself.
 *
 * The other tests in this module pin decisions as pure functions, and [FinishedFileReuseIntegrationTest]
 * proves "did we dial the network" using a dead port. Neither can show what a *response* does to the
 * bytes on disk, which is where the resume bugs actually lived: a `206` spanning the wrong offset, and
 * a cancel that arrives while no `Call` is on the wire. Both need a server that can lie in the exact
 * way RFC 7233 warns about, so this file adds one instead of asserting on an enum.
 */
class ResumeAgainstServerTest {

    private val body = ByteArray(4096) { (it * 7 and 0xFF).toByte() }
    private val resumeFrom = 500L
    private val scopedName = AssetFileName.scoped("owner", "repo", "app.apk")
    private val identity =
        AssetIdentity(assetId = 42L, digest = "sha256:" + sha256Hex(body), size = body.size.toLong())

    /**
     * The headline guarantee: a `206` whose `Content-Range` starts at the offset we asked for is a
     * real resume, and the file that lands is the asset.
     */
    @Test
    fun a_206_at_the_requested_offset_resumes_and_lands_the_whole_asset() =
        runBlocking {
            withServer(Mode.HONOUR_RANGE) { server ->
                withTempDownloadsDir { dir ->
                    writeResumablePartial(dir)
                    val downloader = newDownloader(dir)

                    downloader
                        .download(
                            url = server.url,
                            suggestedFileName = scopedName,
                            bypassMirror = true,
                            identity = identity,
                        ).toList()

                    assertContentEquals(body, File(dir, scopedName).readBytes())
                    assertEquals(listOf("bytes=$resumeFrom-"), server.ranges)
                }
            }
        }

    /**
     * The defect this change fixes, expressed as bytes rather than as an enum.
     *
     * The server answers the ranged request with a *legal-looking* `206` that names the wrong span,
     * which RFC 7233 §4.1 warns a client cannot rely on. Appending that body onto the partial would
     * produce a file that is longer than the asset and correct nowhere. The downloader must instead
     * notice the offset and re-request from zero — which is what a following request with no `Range`
     * header proves.
     */
    @Test
    fun a_206_naming_a_different_span_restarts_instead_of_splicing() =
        runBlocking {
            withServer(Mode.ANSWER_206_FOR_THE_WRONG_SPAN) { server ->
                withTempDownloadsDir { dir ->
                    writeResumablePartial(dir)
                    val downloader = newDownloader(dir)

                    downloader
                        .download(
                            url = server.url,
                            suggestedFileName = scopedName,
                            bypassMirror = true,
                            identity = identity,
                        ).toList()

                    assertContentEquals(
                        body,
                        File(dir, scopedName).readBytes(),
                        "the asset, not our prefix spliced onto a body that did not continue it",
                    )
                    assertEquals(
                        "bytes=$resumeFrom-",
                        server.ranges.first(),
                        "the partial must genuinely have been offered for resume",
                    )
                    assertNull(
                        server.ranges.getOrNull(1),
                        "the restart must re-request without a Range header",
                    )
                }
            }
        }

    /**
     * A cancel that lands while the loop is sleeping between attempts used to be swallowed: the stale
     * `Call` from the failed attempt was still mapped, so `cancel()` hit a dead call and the loop woke
     * up and started the next attempt anyway.
     *
     * The server fails the first request so the loop enters `PartialRetry.backoffMillis(0)` — 1000 ms,
     * per [PartialRetry] — and the cancel is issued 300 ms in, i.e. deliberately inside that window and
     * after the 500 has been delivered. The verdict is the request count: a retry that reached the
     * server would make it 2.
     */
    @Test
    fun a_cancel_during_the_retry_backoff_stops_the_transfer() =
        runBlocking {
            withTimeout(30_000) {
                withServer(Mode.FAIL_FIRST_REQUEST) { server ->
                    withTempDownloadsDir { dir ->
                        val downloader = newDownloader(dir)

                        var failure: Throwable? = null
                        val job =
                            launch(Dispatchers.IO) {
                                try {
                                    downloader
                                        .download(
                                            url = server.url,
                                            suggestedFileName = scopedName,
                                            bypassMirror = true,
                                            identity = identity,
                                        ).toList()
                                } catch (e: Throwable) {
                                    failure = e
                                }
                            }

                        assertTrue(
                            server.awaitFirstRequest(10_000),
                            "the first attempt must reach the server",
                        )
                        Thread.sleep(BACKOFF_WINDOW_PROBE_MILLIS)

                        assertTrue(
                            downloader.cancelDownload(scopedName),
                            "a live transfer existed, so the pause must be accepted",
                        )

                        job.join()

                        assertTrue(
                            failure != null,
                            "the transfer must end, not run on to completion after being cancelled",
                        )
                        assertEquals(
                            1,
                            server.requestCount,
                            "the retry must never reach the server",
                        )
                        assertTrue(
                            !File(dir, scopedName).exists(),
                            "nothing was downloaded, so no finished file may be left behind",
                        )
                    }
                }
            }
        }

    /**
     * A source with no `Range` support at all. RFC 7233 §4.4 is explicit that this is allowed —
     * "servers are free to ignore Range, many implementations will simply respond with the entire
     * selected representation in a 200 (OK) response" — so the downloader must handle it rather than
     * assume a 216-shaped reply.
     *
     * The guarantee: we *do* offer the partial (`Range: bytes=500-` is really sent), the server
     * answers `200` with the whole entity, and the downloader rewrites from zero. The verdict is the
     * file's length as much as its contents: appending the 4096-byte body onto the 500-byte partial
     * would leave 4596 bytes, so a byte-for-byte match also proves nothing was spliced.
     */
    @Test
    fun a_source_without_range_support_rewrites_the_partial_instead_of_splicing() =
        runBlocking {
            withServer(Mode.IGNORE_RANGE) { server ->
                withTempDownloadsDir { dir ->
                    writeResumablePartial(dir)
                    val downloader = newDownloader(dir)

                    downloader
                        .download(
                            url = server.url,
                            suggestedFileName = scopedName,
                            bypassMirror = true,
                            identity = identity,
                        ).toList()

                    assertContentEquals(
                        body,
                        File(dir, scopedName).readBytes(),
                        "a 200 says the server ignored Range, so the partial had to be rewritten",
                    )
                    assertEquals(
                        "bytes=$resumeFrom-",
                        server.ranges.first(),
                        "the partial must genuinely have been offered for resume first",
                    )
                    assertEquals(
                        1,
                        server.requestCount,
                        "the fallback is a rewrite, not an error, so it must not need a retry",
                    )
                    assertTrue(
                        !File(dir, PartialNaming.meta(scopedName)).exists(),
                        "completing the transfer consumes the sidecar",
                    )
                }
            }
        }

    // ---------------------------------------------------------------------------------------------
    // Fixture
    // ---------------------------------------------------------------------------------------------

    private enum class Mode {
        /** A faithful server: `206` with `Content-Range` starting exactly where asked. */
        HONOUR_RANGE,

        /** A server that answers a ranged request with a `206` describing the whole entity. */
        ANSWER_206_FOR_THE_WRONG_SPAN,

        /** A server with no `Range` support: every request gets the whole entity as `200`. */
        IGNORE_RANGE,

        /** Every request fails with `500` until the first one has been answered. */
        FAIL_FIRST_REQUEST,
    }

    private class RangeServer(
        private val body: ByteArray,
        private val mode: Mode,
    ) {
        private val pool = Executors.newFixedThreadPool(4)
        private val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                executor = pool
                createContext("/app.apk") { exchange -> respond(exchange) }
            }
        private val requestLog = Collections.synchronizedList(mutableListOf<String?>())
        private val arrival = CountDownLatch(1)

        val url: String get() = "http://127.0.0.1:${server.address.port}/app.apk"
        val requestCount: Int get() = requestLog.size

        /** The `Range` header of every request, in arrival order; null means none was sent. */
        val ranges: List<String?> get() = requestLog.toList()

        fun start() = server.start()

        fun stop() {
            server.stop(0)
            pool.shutdownNow()
        }

        /** Waits for the first request. [requestCount] covers any expectation beyond that. */
        fun awaitFirstRequest(timeoutMillis: Long): Boolean =
            arrival.await(timeoutMillis, TimeUnit.MILLISECONDS)

        private fun respond(exchange: HttpExchange) {
            val range = exchange.requestHeaders.getFirst("Range")
            requestLog.add(range)
            if (requestLog.size == 1) arrival.countDown()

            if (mode == Mode.FAIL_FIRST_REQUEST && requestLog.size == 1) {
                exchange.sendResponseHeaders(500, -1)
                exchange.close()
                return
            }

            // No Range support: the header is accepted and ignored, which is what a plain static
            // host does. Nothing in the reply even acknowledges that a range was requested.
            if (mode == Mode.IGNORE_RANGE) {
                send(exchange, 200, body, contentRange = null)
                return
            }

            val requested = range?.let { RANGE_HEADER.find(it)?.groupValues?.get(1)?.toLong() }
            if (requested == null) {
                send(exchange, 200, body, contentRange = null)
                return
            }
            when (mode) {
                // Exactly the lie RFC 7233 §4.1 warns about: a 206 that does not describe the range
                // we asked for. It is well-formed, so only the offset reveals it.
                Mode.ANSWER_206_FOR_THE_WRONG_SPAN ->
                    send(exchange, 206, body, contentRange = "bytes 0-${body.size - 1}/${body.size}")

                else ->
                    send(
                        exchange,
                        206,
                        body.copyOfRange(requested.toInt(), body.size),
                        contentRange = "bytes $requested-${body.size - 1}/${body.size}",
                    )
            }
        }

        private fun send(exchange: HttpExchange, code: Int, payload: ByteArray, contentRange: String?) {
            if (contentRange != null) exchange.responseHeaders.add("Content-Range", contentRange)
            exchange.sendResponseHeaders(code, payload.size.toLong())
            exchange.responseBody.use { it.write(payload) }
        }
    }

    private inline fun withServer(mode: Mode, block: (RangeServer) -> Unit) {
        val server = RangeServer(body, mode)
        server.start()
        try {
            block(server)
        } finally {
            server.stop()
        }
    }

    /** A partial holding the first [resumeFrom] bytes, with the sidecar that lets it be attributed. */
    private fun writeResumablePartial(dir: File) {
        File(dir, PartialNaming.part(scopedName)).writeBytes(body.copyOfRange(0, resumeFrom.toInt()))
        File(dir, PartialNaming.meta(scopedName)).writeText(
            PartialIdentity(assetId = 42L, digest = identity.digest, size = identity.size)
                .serialize(),
        )
    }

    private fun newDownloader(dir: File) =
        DesktopDownloader(
            files = FakeFileLocations(dir),
            tokenStore = FakeTokenStore(),
            digestVerifier = DesktopDigestVerifier(),
        )

    private suspend fun withTempDownloadsDir(block: suspend (File) -> Unit) {
        val dir = Files.createTempDirectory("komi-resume-server").toFile()
        try {
            block(dir)
        } finally {
            dir.deleteRecursively()
        }
    }

    private fun sha256Hex(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }

    private companion object {
        val RANGE_HEADER = Regex("""bytes=(\d+)-""")

        /**
         * A third of [PartialRetry]'s first backoff, derived rather than written out so that a
         * change to the schedule cannot silently push the cancel outside the window this test
         * targets. Late enough that the failure has been delivered and the loop is inside `delay`,
         * early enough to stay well within it.
         */
        val BACKOFF_WINDOW_PROBE_MILLIS = PartialRetry.backoffMillis(0) / 3
    }
}
