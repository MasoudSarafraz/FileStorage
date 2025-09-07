using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public sealed class FileStorage2 : IDisposable
{
    private readonly string _StoragePath;
    private readonly uint _DeleteEveryHours;
    private readonly ConcurrentDictionary<Guid, int> _ActiveRefs = new ConcurrentDictionary<Guid, int>();
    private readonly Timer _CleanupTimer;
    private readonly ManualResetEventSlim _CleanupCompleted = new ManualResetEventSlim(true);
    private readonly object _cleanupLock = new object();
    private readonly double _FreeSpaceBufferRatio = 0.1; // 10% buffer for disk space
    private int _CleanupRunning;
    private int _Disposed;
    private static readonly ThreadLocal<Random> _Random = new ThreadLocal<Random>(() => new Random(Guid.NewGuid().GetHashCode()));
    private const int BufferSize = 16384; // 16KB
    private const long MaxFileSize = 100 * 1024 * 1024; // 100 MB

    public FileStorage2(string sPath, uint iDeleteEveryHours = 1)
    {
        if (string.IsNullOrWhiteSpace(sPath))
            throw new ArgumentNullException(nameof(sPath));
        _StoragePath = sPath;
        _DeleteEveryHours = iDeleteEveryHours;
        Directory.CreateDirectory(sPath);
        var oDelay = TimeSpan.FromHours(iDeleteEveryHours);
        var iMs = (int)Math.Min((long)oDelay.TotalMilliseconds, int.MaxValue);
        _CleanupTimer = new Timer(OnCleanupTimer, null, iMs, iMs);
    }

    public Guid SaveFile(byte[] oFileData, string sBase64 = null)
    {
        CheckDisposed();
        if ((oFileData == null || oFileData.Length == 0) && string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");
        oFileData = oFileData ?? Convert.FromBase64String(sBase64);
        EnsureDiskSpace(oFileData.Length);
        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);

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
                    BufferSize))
                {
                    oFs.Write(oFileData, 0, oFileData.Length);
                    oFs.Flush(true);
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

    public Guid SaveFile(Stream oFileStream)
    {
        CheckDisposed();
        if (oFileStream == null)
            throw new ArgumentNullException(nameof(oFileStream));

        if (oFileStream.CanSeek)
            EnsureDiskSpace(oFileStream.Length);
        else
            EnsureDiskSpace(MaxFileSize);

        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);
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
                    BufferSize))
                {
                    oFileStream.CopyTo(oFs);
                    oFs.Flush(true);
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

    public async Task<Guid> SaveFileAsync(byte[] oFileData, string sBase64 = null, CancellationToken oCt = default(CancellationToken))
    {
        CheckDisposed();
        if ((oFileData == null || oFileData.Length == 0) && string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");
        oFileData = oFileData ?? Convert.FromBase64String(sBase64);
        EnsureDiskSpace(oFileData.Length);
        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);
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
                    BufferSize,
                    true))
                {
                    await oFs.WriteAsync(oFileData, 0, oFileData.Length, oCt).ConfigureAwait(false);
                    oFs.Flush(true);
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

    public async Task<Guid> SaveFileAsync(Stream oFileStream, CancellationToken oCt = default(CancellationToken))
    {
        CheckDisposed();
        if (oFileStream == null)
            throw new ArgumentNullException(nameof(oFileStream));

        if (oFileStream.CanSeek)
            EnsureDiskSpace(oFileStream.Length);
        else
            EnsureDiskSpace(MaxFileSize);

        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);
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
                    BufferSize,
                    true))
                {
                    await oFileStream.CopyToAsync(oFs, BufferSize, oCt).ConfigureAwait(false);
                    oFs.Flush(true);
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
                BufferSize,
                false);
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
        try
        {
            return await Task.Run(() => new FileStream(
                sFilePath,
                FileMode.Open,
                FileAccess.Read,
                FileShare.Read | FileShare.Delete,
                BufferSize,
                true), oCt).ConfigureAwait(false);
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
        if (iFileSize > MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {MaxFileSize} bytes).");

        IncrementRef(oFileId);
        try
        {
            return ExecuteWithRetry(() =>
            {
                using (var oStream = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    BufferSize,
                    false))
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
        if (iFileSize > MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {MaxFileSize} bytes).");

        IncrementRef(oFileId);
        try
        {
            return await ExecuteWithRetryAsync(async () =>
            {
                using (var oStream = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    BufferSize,
                    true))
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
        lock (_cleanupLock)
        {
            var oCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
            try
            {
                var oDirInfo = new DirectoryInfo(_StoragePath);

                foreach (var oFileInfo in oDirInfo.GetFiles("*.dat"))
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

                foreach (var oFileInfo in oDirInfo.GetFiles("*.tmp"))
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
        _ActiveRefs.AddOrUpdate(oFileId, 1, (key, value) => value + 1);
    }

    private void DecrementRef(Guid oFileId)
    {
        _ActiveRefs.AddOrUpdate(oFileId, 0, (key, value) => value > 0 ? value - 1 : 0);

        // حذف کلید اگر مقدار به صفر رسید
        if (_ActiveRefs.TryGetValue(oFileId, out int iCount) && iCount == 0)
        {
            _ActiveRefs.TryRemove(oFileId, out _);
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
            try
            {
                return await oFunc().ConfigureAwait(false);
            }
            catch (IOException) when (i < iMaxRetries - 1)
            {
                if (oCt.IsCancellationRequested)
                    break;

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
        var oDriveInfo = new DriveInfo(Path.GetPathRoot(_StoragePath));
        long lRequiredWithBuffer = (long)(lRequiredSpace * (1 + _FreeSpaceBufferRatio));
        if (oDriveInfo.AvailableFreeSpace < lRequiredWithBuffer)
            throw new IOException("Not enough disk space available.");
    }

    private void SafeDeleteFile(string sPath)
    {
        try
        {
            if (File.Exists(sPath))
                File.Delete(sPath);
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