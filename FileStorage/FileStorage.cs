using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public sealed class FileStorage : IDisposable
{
    private readonly string _StoragePath;
    private readonly uint _DeleteEveryHours;
    private readonly ConcurrentDictionary<Guid, int> _ActiveRefs = new ConcurrentDictionary<Guid, int>();
    private readonly Timer _CleanupTimer;
    private readonly ManualResetEventSlim _CleanupCompleted = new ManualResetEventSlim(true);
    private readonly object _CleanupLock = new object();
    private readonly object _RefLock = new object();
    private readonly double _FreeSpaceBufferRatio = 0.1;//جهت در نظر گرفتن 10% فضای بیشتر برای ذخیره فایل
    private int _CleanupRunning;
    private int _Disposed;
    private static readonly ThreadLocal<Random> _Random = new ThreadLocal<Random>(() => new Random(Guid.NewGuid().GetHashCode()));
    private const int _DefaultBufferSize = 8192;
    private const long _MaxFileSize = 100 * 1024 * 1024;
    private readonly DriveInfo _DriveInfo;

    public FileStorage(string sPath, uint iDeleteEveryHours = 1)
    {
        if (string.IsNullOrWhiteSpace(sPath))
            throw new ArgumentNullException(nameof(sPath));

        _StoragePath = sPath;
        _DeleteEveryHours = iDeleteEveryHours;
        Directory.CreateDirectory(sPath);
        _DriveInfo = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(sPath)));

        var oDelay = TimeSpan.FromHours(iDeleteEveryHours);
        var iMs = (int)Math.Min((long)oDelay.TotalMilliseconds, int.MaxValue);
        _CleanupTimer = new Timer(OnCleanupTimer, null, iMs, iMs);
    }
    public Guid SaveFile(Stream oFileStream)
    {
        return SaveFileCore(oFileStream);
    }
    public Guid SaveFile(byte[] oFileData)
    {
        CheckDisposed();
        if (oFileData == null || oFileData.Length == 0)
            throw new ArgumentException("File data is empty");

        using (var oStream = new MemoryStream(oFileData))
        {
            return SaveFileCore(oStream, oFileData.Length);
        }
    }
    public Guid SaveFile(string sBase64)
    {
        CheckDisposed();
        if (string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");

        var oFileData = Convert.FromBase64String(sBase64);
        return SaveFile(oFileData);
    }
    public async Task<Guid> SaveFileAsync(Stream oFileStream, CancellationToken oCt = default(CancellationToken))
    {
        return await SaveFileCoreAsync(oFileStream, null, oCt).ConfigureAwait(false);
    }
    public async Task<Guid> SaveFileAsync(byte[] oFileData, CancellationToken oCt = default(CancellationToken))
    {
        CheckDisposed();
        if (oFileData == null || oFileData.Length == 0)
            throw new ArgumentException("File data is empty");
        using (var oStream = new MemoryStream(oFileData))
        {
            return await SaveFileCoreAsync(oStream, oFileData.Length, oCt).ConfigureAwait(false);
        }
    }
    public async Task<Guid> SaveFileAsync(string sBase64, CancellationToken oCt = default(CancellationToken))
    {
        CheckDisposed();
        if (string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");
        var oFileData = Convert.FromBase64String(sBase64);
        return await SaveFileAsync(oFileData, oCt).ConfigureAwait(false);
    }
    public Stream GetFile(Guid oFileId)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(oFileId);
        try
        {
            return new FileStream(
                sFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read | FileShare.Delete,
                _DefaultBufferSize,
                FileOptions.RandomAccess | FileOptions.SequentialScan);
        }
        catch (FileNotFoundException ex)
        {
            throw new FileNotFoundException("File not found.", sFilePath, ex);
        }
    }

    public async Task<Stream> GetFileAsync(Guid oFileId, CancellationToken oCt = default(CancellationToken))
    {
        CheckDisposed();
        oCt.ThrowIfCancellationRequested();
        var sFilePath = GetFilePath(oFileId);

        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);

        try
        {
            return new FileStream(
                sFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read | FileShare.Delete,
                _DefaultBufferSize,
                FileOptions.Asynchronous | FileOptions.SequentialScan);
        }
        catch (FileNotFoundException ex)
        {
            throw new FileNotFoundException("File not found.", sFilePath, ex);
        }
    }

    public byte[] GetFileBytes(Guid oFileId)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(oFileId);

        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);

        var iFileSize = new FileInfo(sFilePath).Length;
        if (iFileSize > _MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes).");

        IncrementRef(oFileId);
        try
        {
            return ExecuteWithRetry(() =>
            {
                int iBufferSize = GetOptimaizBufferSize(iFileSize);
                using (var oStream = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    iBufferSize,
                    FileOptions.SequentialScan))
                {
                    var oBuffer = new byte[oStream.Length];
                    int iTotalRead = 0;
                    while (iTotalRead < oBuffer.Length)
                    {
                        int iRead = oStream.Read(oBuffer, iTotalRead, oBuffer.Length - iTotalRead);
                        if (iRead == 0)
                            throw new IOException("File read incomplete.");
                        iTotalRead += iRead;
                    }
                    return oBuffer;
                }
            });
        }
        finally
        {
            DecrementRef(oFileId);
        }
    }

    public async Task<byte[]> GetFileBytesAsync(Guid oFileId, CancellationToken oCt = default(CancellationToken))
    {
        CheckDisposed();
        var sFilePath = GetFilePath(oFileId);

        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);

        var iFileSize = new FileInfo(sFilePath).Length;
        if (iFileSize > _MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes).");

        IncrementRef(oFileId);
        try
        {
            return await ExecuteWithRetryAsync(async () =>
            {
                int iBufferSize = GetOptimaizBufferSize(iFileSize);
                using (var oStream = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    iBufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    var oBuffer = new byte[oStream.Length];
                    int iTotalRead = 0;
                    while (iTotalRead < oBuffer.Length)
                    {
                        int iRead = await oStream.ReadAsync(oBuffer, iTotalRead, oBuffer.Length - iTotalRead, oCt).ConfigureAwait(false);
                        if (iRead == 0)
                            throw new IOException("File read incomplete.");
                        iTotalRead += iRead;
                    }
                    return oBuffer;
                }
            }, oCt).ConfigureAwait(false);
        }
        finally
        {
            DecrementRef(oFileId);
        }
    }
    private int GetOptimaizBufferSize(long lFileSize)
    {
        if (lFileSize <= 8192) return 8192;
        if (lFileSize <= 65536) return 16384;
        if (lFileSize <= 524288) return 32768;
        return 65536;
    }
    private Guid SaveFileCore(Stream oStream, long? lKnownLength = null)
    {
        CheckDisposed();
        if (oStream == null)
            throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        if (!lKnownLength.HasValue && oStream.CanSeek)
            EnsureDiskSpace(oStream.Length);
        else
            EnsureDiskSpace(lFileSize);
        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue ? GetOptimaizBufferSize(lKnownLength.Value) : _DefaultBufferSize;
        IncrementRef(oFileId);
        try
        {
            ExecuteWithRetry(() =>
            {
                using (var oFs = new FileStream(
                    sTempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    iBufferSize,
                    FileOptions.SequentialScan))
                {
                    oStream.CopyTo(oFs, iBufferSize);
                }
                File.Move(sTempPath, sFinalPath);
            });
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            DecrementRef(oFileId);
            throw;
        }

        return oFileId;
    }
    private async Task<Guid> SaveFileCoreAsync(Stream oStream, long? lKnownLength, CancellationToken oCt = default(CancellationToken))
    {
        CheckDisposed();
        if (oStream == null)
            throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        if (!lKnownLength.HasValue && oStream.CanSeek)
            EnsureDiskSpace(oStream.Length);
        else
            EnsureDiskSpace(lFileSize);
        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);

        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue
            ? GetOptimaizBufferSize(lKnownLength.Value)
            : _DefaultBufferSize;
        IncrementRef(oFileId);
        try
        {
            await ExecuteWithRetryAsync(async () =>
            {
                using (var oFs = new FileStream(
                    sTempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    iBufferSize,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    await oStream.CopyToAsync(oFs, iBufferSize, oCt).ConfigureAwait(false);
                    await oFs.FlushAsync(oCt).ConfigureAwait(false);
                }
                File.Move(sTempPath, sFinalPath);
            }, oCt).ConfigureAwait(false);
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            DecrementRef(oFileId);
            throw;
        }

        return oFileId;
    }
    private void OnCleanupTimer(object oState)
    {
        if (Interlocked.CompareExchange(ref _CleanupRunning, 1, 0) != 0)
            return;

        _CleanupCompleted.Reset();
        try
        {
            CleanupOldFiles();
        }
        catch (Exception ex)
        {
            Debug.WriteLine($"Cleanup failed: {ex.Message}");
        }
        finally
        {
            Interlocked.Exchange(ref _CleanupRunning, 0);
            _CleanupCompleted.Set();
        }
    }

    private void CleanupOldFiles()
    {
        lock (_CleanupLock)
        {
            var oCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
            try
            {
                var oDirInfo = new DirectoryInfo(_StoragePath);
                foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.dat"))
                {
                    try
                    {
                        var sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                        if (!Guid.TryParseExact(sFileName, "N", out var oFileId))
                            continue;

                        if (_ActiveRefs.TryGetValue(oFileId, out int iCount) && iCount > 0)
                            continue;

                        if (oFileInfo.CreationTimeUtc < oCutoff)
                        {
                            SafeDeleteFile(oFileInfo.FullName);
                        }
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"Error during file cleanup: {ex.Message}");
                    }
                }

                foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.tmp"))
                {
                    try
                    {
                        SafeDeleteFile(oFileInfo.FullName);
                    }
                    catch (Exception ex)
                    {
                        Debug.WriteLine($"Error during temp file cleanup: {ex.Message}");
                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Global cleanup error: {ex.Message}");
            }
        }
    }

    private string GetFilePath(Guid oFileId) => Path.Combine(_StoragePath, oFileId.ToString("N") + ".dat");
    private string GetTempPath(Guid oFileId) => Path.Combine(_StoragePath, oFileId.ToString("N") + ".tmp");

    private void IncrementRef(Guid oFileId)
    {
        lock (_RefLock)
        {
            _ActiveRefs.AddOrUpdate(oFileId, 1, (key, value) => value + 1);
        }
    }

    private void DecrementRef(Guid oFileId)
    {
        lock (_RefLock)
        {
            int newCount = _ActiveRefs.AddOrUpdate(oFileId, 0, (key, value) => value > 0 ? value - 1 : 0);
            if (newCount == 0)
            {
                _ActiveRefs.TryRemove(oFileId, out _);
            }
        }
    }

    private T ExecuteWithRetry<T>(Func<T> oFunc, int iMaxRetries = 5, int iBaseDelay = 20)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                return oFunc();
            }
            catch (IOException) when (i < iMaxRetries - 1)
            {
                Thread.Sleep(iBaseDelay * (1 << i) + _Random.Value.Next(5, 50));
            }
        }
        throw new IOException("File operation failed after retries");
    }

    private void ExecuteWithRetry(Action oAction, int iMaxRetries = 5, int iBaseDelay = 20)
    {
        ExecuteWithRetry(() =>
        {
            oAction();
            return true;
        }, iMaxRetries, iBaseDelay);
    }

    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> oFunc, CancellationToken oCt = default(CancellationToken), int iMaxRetries = 5, int iBaseDelay = 20)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            oCt.ThrowIfCancellationRequested();
            try
            {
                return await oFunc().ConfigureAwait(false);
            }
            catch (IOException) when (i < iMaxRetries - 1)
            {
                var iDelay = iBaseDelay * (1 << i) + _Random.Value.Next(5, 50);
                await Task.Delay(iDelay, oCt).ConfigureAwait(false);
            }
        }
        throw new IOException("Async file operation failed after retries");
    }

    private async Task ExecuteWithRetryAsync(Func<Task> oAction, CancellationToken oCt = default(CancellationToken), int iMaxRetries = 5, int iBaseDelay = 20)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            await oAction().ConfigureAwait(false);
            return true;
        }, oCt, iMaxRetries, iBaseDelay).ConfigureAwait(false);
    }

    private void EnsureDiskSpace(long lRequiredSpace)
    {
        long lRequiredWithBuffer = (long)(lRequiredSpace * (1 + _FreeSpaceBufferRatio));
        if (_DriveInfo.AvailableFreeSpace < lRequiredWithBuffer)
            throw new IOException("Not enough disk space available.");
    }

    private void SafeDeleteFile(string sPath)
    {
        try
        {
            File.Delete(sPath);
        }
        catch (FileNotFoundException)
        {
            // No Matter
        }
        catch (IOException ex)
        {
            Debug.WriteLine($"Error deleting file: {ex.Message}");
        }
        catch (UnauthorizedAccessException ex)
        {
            Debug.WriteLine($"Access denied deleting file: {ex.Message}");
        }
    }

    private void CheckDisposed()
    {
        if (Interlocked.CompareExchange(ref _Disposed, 0, 0) == 1)
            throw new ObjectDisposedException("FileStorage2");
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _Disposed, 1, 0) != 0)
            return;

        _CleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        _CleanupCompleted.Wait();
        _CleanupTimer?.Dispose();
        _ActiveRefs.Clear();
        _CleanupCompleted.Dispose();
    }
}
