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
    private long _CleanupRunning;
    private bool _Disposed;
    private static readonly ThreadLocal<Random> _Random = new ThreadLocal<Random>(() => new Random());
    private const long _MaxFileSizeForBytes = 100 * 1024 * 1024; // 100 MB

    public FileStorage2(string sPath, uint iDeleteEveryHours = 1)
    {
        if (string.IsNullOrWhiteSpace(sPath))
            throw new ArgumentNullException(nameof(sPath));

        this._StoragePath = sPath;
        this._DeleteEveryHours = iDeleteEveryHours;

        Directory.CreateDirectory(sPath);
        var oDelay = TimeSpan.FromHours(iDeleteEveryHours);
        var iMs = (int)Math.Min((long)oDelay.TotalMilliseconds, int.MaxValue);
        this._CleanupTimer = new Timer(OnCleanupTimer, null, iMs, iMs);
    }

    public Guid SaveFile(byte[] oFileData, string sBase64 = null)
    {
        if ((oFileData == null || oFileData.Length == 0) && string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");

        oFileData = oFileData ?? Convert.FromBase64String(sBase64);
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
                    bufferSize: 81920,
                    useAsync: false))
                {
                    oFs.Write(oFileData, 0, oFileData.Length);
                }

                File.Move(sTempPath, sFinalPath);
            });
        }
        catch
        {
            try { if (File.Exists(sTempPath)) File.Delete(sTempPath); } catch { }
            DecrementRef(oFileId);
            throw;
        }

        return oFileId;
    }

    public Guid SaveFile(Stream oFileStream)
    {
        if (oFileStream == null)
            throw new ArgumentNullException(nameof(oFileStream));

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
                    bufferSize: 81920,
                    useAsync: false))
                {
                    oFileStream.CopyTo(oFs);
                }

                File.Move(sTempPath, sFinalPath);
            });
        }
        catch
        {
            try { if (File.Exists(sTempPath)) File.Delete(sTempPath); } catch { }
            DecrementRef(oFileId);
            throw;
        }

        return oFileId;
    }

    public async Task<Guid> SaveFileAsync(byte[] oFileData, string sBase64 = null, CancellationToken oCt = default(CancellationToken))
    {
        if ((oFileData == null || oFileData.Length == 0) && string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");

        oFileData = oFileData ?? Convert.FromBase64String(sBase64);
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
                    bufferSize: 81920,
                    useAsync: true))
                {
                    await oFs.WriteAsync(oFileData, 0, oFileData.Length, oCt).ConfigureAwait(false);
                }

                File.Move(sTempPath, sFinalPath);
            }, oCt).ConfigureAwait(false);
        }
        catch
        {
            try { if (File.Exists(sTempPath)) File.Delete(sTempPath); } catch { }
            DecrementRef(oFileId);
            throw;
        }

        return oFileId;
    }

    public async Task<Guid> SaveFileAsync(Stream oFileStream, CancellationToken oCt = default(CancellationToken))
    {
        if (oFileStream == null)
            throw new ArgumentNullException(nameof(oFileStream));

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
                    bufferSize: 81920,
                    useAsync: true))
                {
                    await oFileStream.CopyToAsync(oFs, 81920, oCt).ConfigureAwait(false);
                }

                File.Move(sTempPath, sFinalPath);
            }, oCt).ConfigureAwait(false);
        }
        catch
        {
            try { if (File.Exists(sTempPath)) File.Delete(sTempPath); } catch { }
            DecrementRef(oFileId);
            throw;
        }

        return oFileId;
    }

    public Stream GetFile(Guid oFileId)
    {
        var sFilePath = GetFilePath(oFileId);
        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);

        return new FileStream(
            sFilePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read | FileShare.Delete,
            bufferSize: 81920,
            useAsync: true);
    }

    public async Task<Stream> GetFileAsync(Guid oFileId, CancellationToken oCt = default(CancellationToken))
    {
        var sFilePath = GetFilePath(oFileId);
        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);

        return new FileStream(
            sFilePath,
            FileMode.Open,
            FileAccess.Read,
            FileShare.Read | FileShare.Delete,
            bufferSize: 81920,
            useAsync: true);
    }

    public byte[] GetFileBytes(Guid oFileId)
    {
        var sFilePath = GetFilePath(oFileId);
        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);

        var iFileSize = new FileInfo(sFilePath).Length;
        if (iFileSize > _MaxFileSizeForBytes)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSizeForBytes} bytes).");

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
                    bufferSize: 81920,
                    useAsync: false))
                {
                    var oBuffer = new byte[oStream.Length];
                    int iRead = oStream.Read(oBuffer, 0, oBuffer.Length);
                    if (iRead != oBuffer.Length)
                        throw new IOException("File read incomplete.");
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
        var sFilePath = GetFilePath(oFileId);
        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);

        var iFileSize = new FileInfo(sFilePath).Length;
        if (iFileSize > _MaxFileSizeForBytes)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSizeForBytes} bytes).");

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
                    bufferSize: 81920,
                    useAsync: true))
                {
                    var oBuffer = new byte[oStream.Length];
                    int iRead = await oStream.ReadAsync(oBuffer, 0, oBuffer.Length, oCt).ConfigureAwait(false);
                    if (iRead != oBuffer.Length)
                        throw new IOException("File read incomplete.");
                    return oBuffer;
                }
            }, oCt).ConfigureAwait(false);
        }
        finally
        {
            DecrementRef(oFileId);
        }
    }

    private void OnCleanupTimer(object state)
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
            LogError("Cleanup failed", ex);
        }
        finally
        {
            Interlocked.Exchange(ref _CleanupRunning, 0);
            _CleanupCompleted.Set();
        }
    }

    private void CleanupOldFiles()
    {
        var dCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
        try
        {
            foreach (var sPattern in new[] { "*.dat", "*.tmp" })
            {
                foreach (var sFilePath in Directory.GetFiles(_StoragePath, sPattern))
                {
                    try
                    {
                        var sFileName = Path.GetFileNameWithoutExtension(sFilePath);
                        if (!Guid.TryParse(sFileName, out var oFileId))
                            continue;

                        // اگر فایل در حال استفاده (در Save) باشد، حذف نشود
                        if (_ActiveRefs.ContainsKey(oFileId))
                            continue;

                        DateTime dCreationTime;
                        try
                        {
                            dCreationTime = File.GetCreationTimeUtc(sFilePath);
                        }
                        catch (IOException)
                        {
                            continue;
                        }
                        catch (UnauthorizedAccessException)
                        {
                            continue;
                        }

                        if (dCreationTime < dCutoff)
                        {
                            try
                            {
                                File.Delete(sFilePath);
                            }
                            catch (IOException) { }
                            catch (UnauthorizedAccessException) { }
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError("Error during file cleanup", ex);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            LogError("Global cleanup error", ex);
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
        int current;
        do
        {
            current = _ActiveRefs.GetOrAdd(oFileId, 0);
            if (current == 0) break;
        } while (!_ActiveRefs.TryUpdate(oFileId, current - 1, current));

        if (current == 1)
            _ActiveRefs.TryRemove(oFileId, out _);
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

    private void LogError(string sMessage, Exception oEx)
    {
        Debug.WriteLine($"{sMessage}: {oEx.Message}");
    }

    public void Dispose()
    {
        if (_Disposed) return;
        _Disposed = true;

        _CleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
        _CleanupCompleted.Wait();
        _CleanupTimer?.Dispose();
        _ActiveRefs.Clear();
    }
}