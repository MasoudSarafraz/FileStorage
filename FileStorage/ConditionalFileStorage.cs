// برای استفاده در محیط‌های وب و ویندوزی
// در پروژه‌های وب، باید نماد کامپایل شرطی "WEB" را تعریف کنید
// در پروژه‌های ویندوزی، نیازی به تعریف این نماد نیست
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security;
using System.Security.Principal;
using System.Threading;
using System.Threading.Tasks;

#if WEB
using System.Web.Hosting;
#endif
public sealed class ConditionalFileStorage : IDisposable
#if WEB
    , IRegisteredObject
#endif
{
    private readonly string _StoragePath;
    private readonly uint _DeleteEveryHours;
    private readonly ConcurrentDictionary<Guid, int> _ActiveRefs = new ConcurrentDictionary<Guid, int>();
    private Timer _CleanupTimer;
    private readonly ManualResetEventSlim _CleanupCompleted = new ManualResetEventSlim(true);
    private readonly object _RefsLock = new object();
    private readonly double _FreeSpaceBufferRatio = 0.1;
    private volatile int _CleanupRunning;
    private volatile int _Disposed;
    private static readonly ThreadLocal<Random> _Random = new ThreadLocal<Random>(() => new Random(Guid.NewGuid().GetHashCode()));
    private const int _DefaultBufferSize = 8192;
    private const long _MaxFileSize = 100 * 1024 * 1024;
    private readonly DriveInfo _DriveInfo;
    private long _LastDiskCheckTime;
    private long _LastFreeSpace;
    private readonly string _LogFilePath;

#if WEB
    private FileStorageRegisteredObject _RegisteredObject;
#endif

    public ConditionalFileStorage(string sPath, uint iDeleteEveryHours = 1)
    {
        if (string.IsNullOrWhiteSpace(sPath))
            throw new ArgumentNullException(nameof(sPath));
        try
        {
            _StoragePath = Path.GetFullPath(sPath);
            if (!Path.IsPathRooted(_StoragePath))
                throw new ArgumentException("Path must be rooted", nameof(sPath));
        }
        catch (Exception ex) when (ex is ArgumentException || ex is PathTooLongException || ex is NotSupportedException)
        {
            throw new ArgumentException("Invalid storage path", nameof(sPath), ex);
        }
        _DeleteEveryHours = iDeleteEveryHours;
        Directory.CreateDirectory(_StoragePath);
        _DriveInfo = new DriveInfo(Path.GetPathRoot(_StoragePath));
        _LogFilePath = Path.Combine(_StoragePath, "FileStorageErrors.log");
        // بررسی مجوزهای دسترسی
        CheckPermissions();

#if WEB
        // ثبت در هاستینگ محیط برای جلوگیری از خاموش شدن تایمر
        if (HostingEnvironment.IsHosted)
        {
            _RegisteredObject = new FileStorageRegisteredObject(this);
            HostingEnvironment.RegisterObject(_RegisteredObject);
        }
#endif
        // ایجاد تایمر با اجرای اولیه بلافاصله
        var oDelay = TimeSpan.FromHours(iDeleteEveryHours);
        int iMs = (int)Math.Min((long)oDelay.TotalMilliseconds, int.MaxValue);
        _CleanupTimer = new Timer(OnCleanupTimer, null, 30000, iMs); // 30 ثانیه = 30000 میلی‌ثانیه
        LogMessage($"FileStorage initialized. Path: {_StoragePath}, Cleanup interval: {iDeleteEveryHours} hours");
    }

    private void CheckPermissions()
    {
        try
        {
            string testFile = Path.Combine(_StoragePath, "permission_test.tmp");
            File.WriteAllText(testFile, "test");
            // بررسی امکان حذف فایل
            File.Delete(testFile);
            LogMessage("Permission check passed successfully");
        }
        catch (Exception ex)
        {
            string currentUser = WindowsIdentity.GetCurrent().Name;
            throw new Exception($"The application does not have write/delete permissions on the storage path: {_StoragePath}. " + $"Current user: {currentUser}. " +
                $"Please grant 'Modify' permissions to the Application Pool identity. Error: {ex.Message}", ex);
        }
    }

    public Guid SaveFile(Stream oFileStream) => SaveFileCore(oFileStream);
    public Guid SaveFile(byte[] oFileData) => SaveFileCore(new MemoryStream(oFileData), oFileData.Length);
    public Guid SaveFile(string sBase64) => SaveFile(Convert.FromBase64String(sBase64));
    public Task<Guid> SaveFileAsync(Stream oFileStream, CancellationToken oCt = default) => SaveFileCoreAsync(oFileStream, null, oCt);
    public Task<Guid> SaveFileAsync(byte[] oFileData, CancellationToken oCt = default) => SaveFileCoreAsync(new MemoryStream(oFileData), oFileData.Length, oCt);
    public Task<Guid> SaveFileAsync(string sBase64, CancellationToken oCt = default) => SaveFileAsync(Convert.FromBase64String(sBase64), oCt);

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

    public async Task<Stream> GetFileAsync(Guid oFileId, CancellationToken oCt = default)
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
                int iBufferSize = GetOptimizedBufferSize(iFileSize);
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

    public async Task<byte[]> GetFileBytesAsync(Guid oFileId, CancellationToken oCt = default)
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
                int iBufferSize = GetOptimizedBufferSize(iFileSize);
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

    private int GetOptimizedBufferSize(long lFileSize)
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
        EnsureDiskSpace(lFileSize);
        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue ? GetOptimizedBufferSize(lKnownLength.Value) : _DefaultBufferSize;

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

    private async Task<Guid> SaveFileCoreAsync(Stream oStream, long? lKnownLength, CancellationToken oCt = default)
    {
        CheckDisposed();
        if (oStream == null)
            throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        EnsureDiskSpace(lFileSize);
        var oFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(oFileId);
        var sFinalPath = GetFilePath(oFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue ? GetOptimizedBufferSize(lKnownLength.Value) : _DefaultBufferSize;
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
        if (_Disposed != 0)
            return;
        if (Interlocked.CompareExchange(ref _CleanupRunning, 1, 0) != 0)
            return;
        _CleanupCompleted.Reset();
        try
        {
            LogMessage("Cleanup timer triggered");
            CleanupOldFiles();
        }
        catch (Exception ex)
        {
            LogMessage($"Cleanup failed: {ex}");
        }
        finally
        {
            Interlocked.Exchange(ref _CleanupRunning, 0);
            _CleanupCompleted.Set();
            // تنظیم مجدد تایمر برای اجرای بعدی
            if (_Disposed == 0)
            {
                int iMs = (int)Math.Min(TimeSpan.FromHours(_DeleteEveryHours).TotalMilliseconds, int.MaxValue);
                _CleanupTimer?.Change(iMs, iMs);
                LogMessage($"Cleanup timer reset for next run in {_DeleteEveryHours} hours");
            }
        }
    }

    private void CleanupOldFiles()
    {
        try
        {
            var oCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
            LogMessage($"Cleanup started. Cutoff time: {oCutoff}");
            var oFilesToDelete = new List<string>();
            var oTempFilesToDelete = new List<string>();
            var oDirInfo = new DirectoryInfo(_StoragePath);
            // جمع‌آوری فایل‌های .dat برای حذف
            foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.dat"))
            {
                try
                {
                    var sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                    if (!Guid.TryParseExact(sFileName, "N", out var oFileId))
                        continue;

                    bool bHasActiveRef;
                    lock (_RefsLock)
                    {
                        bHasActiveRef = _ActiveRefs.TryGetValue(oFileId, out int iCount) && iCount > 0;
                    }

                    if (!bHasActiveRef && oFileInfo.CreationTimeUtc < oCutoff)
                    {
                        oFilesToDelete.Add(oFileInfo.FullName);
                        LogMessage($"File marked for deletion: {oFileInfo.FullName} (Created: {oFileInfo.CreationTimeUtc})");
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($"Error processing file {oFileInfo.Name}: {ex}");
                }
            }
            // جمع‌آوری فایل‌های موقت
            foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.tmp"))
            {
                oTempFilesToDelete.Add(oFileInfo.FullName);
                LogMessage($"Temp file marked for deletion: {oFileInfo.FullName}");
            }
            int iDeletedCount = 0;
            int iFailedCount = 0;
            // حذف فایل‌های .dat
            foreach (var sFilePath in oFilesToDelete)
            {
                if (SafeDeleteFile(sFilePath))
                    iDeletedCount++;
                else
                    iFailedCount++;
            }
            // حذف فایل‌های موقت
            foreach (var sFilePath in oTempFilesToDelete)
            {
                if (SafeDeleteFile(sFilePath))
                    iDeletedCount++;
                else
                    iFailedCount++;
            }
            LogMessage($"Cleanup completed. Deleted: {iDeletedCount}, Failed: {iFailedCount}");
        }
        catch (Exception ex)
        {
            LogMessage($"Global cleanup error: {ex}");
        }
    }

    private string GetFilePath(Guid oFileId) => Path.Combine(_StoragePath, oFileId.ToString("N") + ".dat");
    private string GetTempPath(Guid oFileId) => Path.Combine(_StoragePath, oFileId.ToString("N") + ".tmp");

    private void IncrementRef(Guid oFileId)
    {
        lock (_RefsLock)
        {
            _ActiveRefs.AddOrUpdate(oFileId, 1, (key, value) => value + 1);
        }
    }

    private void DecrementRef(Guid oFileId)
    {
        lock (_RefsLock)
        {
            int iNewValue = _ActiveRefs.AddOrUpdate(oFileId, 0, (key, value) => value - 1);
            if (iNewValue <= 0)
            {
                _ActiveRefs.TryRemove(oFileId, out _);
            }
        }
    }

    private T ExecuteWithRetry<T>(Func<T> oFunc, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                return oFunc();
            }
            catch (Exception ex) when (IsTransientFileError(ex) && i < iMaxRetries - 1)
            {
                Thread.Sleep(iBaseDelay * (1 << i) + _Random.Value.Next(0, 5));
            }
        }
        throw new IOException("File operation failed after retries");
    }

    private void ExecuteWithRetry(Action oAction, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        ExecuteWithRetry(() =>
        {
            oAction();
            return true;
        }, iMaxRetries, iBaseDelay);
    }

    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> oFunc, CancellationToken oCt = default, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            oCt.ThrowIfCancellationRequested();
            try
            {
                return await oFunc().ConfigureAwait(false);
            }
            catch (Exception ex) when (IsTransientFileError(ex) && i < iMaxRetries - 1)
            {
                var iDelay = iBaseDelay * (1 << i) + _Random.Value.Next(0, 5);
                await Task.Delay(iDelay, oCt).ConfigureAwait(false);
            }
        }
        throw new IOException("Async file operation failed after retries");
    }

    private async Task ExecuteWithRetryAsync(Func<Task> oAction, CancellationToken oCt = default, int iMaxRetries = 2, int iBaseDelay = 5)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            await oAction().ConfigureAwait(false);
            return true;
        }, oCt, iMaxRetries, iBaseDelay).ConfigureAwait(false);
    }

    private void EnsureDiskSpace(long lRequiredSpace)
    {
        long lNow = Stopwatch.GetTimestamp();
        if (lNow - _LastDiskCheckTime < TimeSpan.TicksPerSecond * 5)
        {
            if (_LastFreeSpace >= lRequiredSpace * (1 + _FreeSpaceBufferRatio))
                return;
        }
        lock (_RefsLock)
        {
            lNow = Stopwatch.GetTimestamp();
            if (lNow - _LastDiskCheckTime >= TimeSpan.TicksPerSecond * 5)
            {
                _LastFreeSpace = _DriveInfo.AvailableFreeSpace;
                _LastDiskCheckTime = lNow;
            }
            long lRequiredWithBuffer = (long)(lRequiredSpace * (1 + _FreeSpaceBufferRatio));
            if (_LastFreeSpace < lRequiredWithBuffer)
                throw new IOException("Not enough disk space available.");
        }
    }

    private bool SafeDeleteFile(string sPath)
    {
        const int iMaxRetries = 3;
        const int iBaseDelayMs = 100;
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                if (!File.Exists(sPath))
                    return true;
                // بررسی ویژگی فقط-خواندنی
                var fileInfo = new FileInfo(sPath);
                if (fileInfo.IsReadOnly)
                {
                    fileInfo.IsReadOnly = false;
                    LogMessage($"Removed read-only attribute from file: {sPath}");
                }
                // تلاش برای حذف فایل
                File.Delete(sPath);
                LogMessage($"Successfully deleted file: {sPath}");
                return true;
            }
            catch (Exception ex) when (ex is FileNotFoundException || ex is DirectoryNotFoundException)
            {
                return true; // فایل وجود ندارد - موفقیت‌آمیز در نظر گرفته می‌شود
            }
            catch (Exception ex) when (i < iMaxRetries - 1 && (ex is IOException || ex is UnauthorizedAccessException))
            {
                LogMessage($"Attempt {i + 1} to delete file '{sPath}' failed: {ex.Message}. Retrying in {iBaseDelayMs * (i + 1)}ms...");
                Thread.Sleep(iBaseDelayMs * (i + 1));
            }
            catch (Exception ex)
            {
                LogMessage($"Critical error deleting file '{sPath}': {ex}");
                return false;
            }
        }

        LogMessage($"Failed to delete file '{sPath}' after {iMaxRetries} attempts.");
        return false;
    }

    private void CheckDisposed()
    {
        if (_Disposed != 0) throw new ObjectDisposedException("FileStorage");
    }

    private bool IsTransientFileError(Exception oEx)
    {
        return oEx is IOException || oEx is UnauthorizedAccessException || oEx is SecurityException;
    }

    private void LogMessage(string message)
    {
        try
        {
            string logEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {message}{Environment.NewLine}";
            File.AppendAllText(_LogFilePath, logEntry);
        }
        catch { }
    }

    public void ForceCleanup()
    {
        OnCleanupTimer(null);
    }

    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _Disposed, 1, 0) != 0)
            return;

        try
        {
            _CleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);

            if (_CleanupRunning != 0)
            {
                _CleanupCompleted.Wait(TimeSpan.FromSeconds(5));
            }
        }
        catch (Exception ex)
        {
            LogMessage($"Error during timer disposal: {ex}");
        }
        finally
        {
            try
            {
                _CleanupTimer?.Dispose();
#if WEB
                if (_RegisteredObject != null)
                {
                    HostingEnvironment.UnregisterObject(_RegisteredObject);
                }
#endif
            }
            catch (Exception ex)
            {
                LogMessage($"Error disposing timer: {ex}");
            }

            _ActiveRefs.Clear();
            _CleanupCompleted.Dispose();
        }
    }

#if WEB
    void IRegisteredObject.Stop(bool blnImmediate)
    {
        if (blnImmediate)
        {
            Dispose();
        }
        else
        {
            try
            {
                _CleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                if (_CleanupRunning != 0)
                {
                    _CleanupCompleted.Wait(TimeSpan.FromSeconds(10));
                }
            }
            catch (Exception ex)
            {
                LogError($"Error during graceful shutdown: {ex}");
            }
            finally
            {
                Dispose();
            }
        }
    }

    private class FileStorageRegisteredObject : IRegisteredObject
    {
        private readonly ConditionalFileStorage _Storage;

        public FileStorageRegisteredObject(ConditionalFileStorage storage)
        {
            _Storage = storage;
        }

        public void Stop(bool blnImmediate)
        {
            if (blnImmediate)
            {
                _Storage.Dispose();
            }
            else
            {
                try
                {
                    _Storage._CleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                    if (_Storage._CleanupRunning != 0)
                    {
                        _Storage._CleanupCompleted.Wait(TimeSpan.FromSeconds(10));
                    }
                }
                catch (Exception ex)
                {
                    _Storage.LogError($"Error during graceful shutdown: {ex}");
                }
                finally
                {
                    _Storage.Dispose();
                }
            }
        }
    }
#endif
}