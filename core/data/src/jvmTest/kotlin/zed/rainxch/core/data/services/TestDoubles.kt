package zed.rainxch.core.data.services

import java.io.File
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import zed.rainxch.core.data.data_source.TokenStore
import zed.rainxch.core.data.dto.GithubDeviceTokenSuccessDto

/**
 * The two doubles every downloader test needs, in one place.
 *
 * They were copy-pasted per test file, which means a new method on [FileLocationsProvider] or
 * [TokenStore] would have to be added to each copy and a missed copy would break the build in one
 * file only. Shared here instead.
 */

/** Points both download directories at one temporary directory, so tests can inspect writes. */
internal class FakeFileLocations(private val dir: File) : FileLocationsProvider {
    override fun appDownloadsDir(): String = dir.absolutePath

    override fun userDownloadsDir(): String = dir.absolutePath

    override fun setExecutableIfNeeded(path: String) = Unit

    override fun getCacheSizeBytes(): Long = 0L

    override fun clearCacheFiles(): Boolean = false
}

/** Signed out: no token, so the downloaders send no Authorization header. */
internal class FakeTokenStore : TokenStore {
    override fun tokenFlow(): Flow<GithubDeviceTokenSuccessDto?> = emptyFlow()

    override suspend fun currentToken(): GithubDeviceTokenSuccessDto? = null

    override fun blockingCurrentToken(): GithubDeviceTokenSuccessDto? = null

    override suspend fun save(token: GithubDeviceTokenSuccessDto) = Unit

    override suspend fun clear() = Unit

    override suspend fun isTokenExpired(): Boolean = false
}
