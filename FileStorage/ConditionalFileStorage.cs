using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security;
using System.Security.Principal;
using System.Text;
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
    private readonly object _IndexLock = new object();
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
    private readonly string _IndexFilePath;
    private List<FileIndexEntry> _FileIndex = new List<FileIndexEntry>();
    private DateTime _LastIndexRebuild = DateTime.MinValue;
    private const int _MaxIndexEntries = 10000;
    private const int _IndexRebuildHours = 24;
#if WEB
    private FileStorageRegisteredObject _RegisteredObject;
#endif
    private class FileIndexEntry
    {
        public string sId { get; set; }
        public DateTime dtCreated { get; set; }
    }
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
        _LogFilePath = Path.Combine(_StoragePath, "FileStorageLogs.log");
        _IndexFilePath = Path.Combine(_StoragePath, "FileIndex.json");
        CheckPermissions();
        CleanupTempIndexFiles();
        LoadOrRebuildIndex();
#if WEB
        if (HostingEnvironment.IsHosted)
        {
            _RegisteredObject = new FileStorageRegisteredObject(this);
            HostingEnvironment.RegisterObject(_RegisteredObject);
        }
#endif
        var oDelay = TimeSpan.FromHours(iDeleteEveryHours);
        int iMs = (int)Math.Min((long)oDelay.TotalMilliseconds, int.MaxValue);
        _CleanupTimer = new Timer(OnCleanupTimer, null, 30000, iMs);
        LogError($"FileStorage initialized. Path: {_StoragePath}, Cleanup interval: {iDeleteEveryHours} hours");
    }
    private void LoadOrRebuildIndex()
    {
        lock (_IndexLock)
        {
            try
            {
                if (File.Exists(_IndexFilePath))
                {
                    string sJson = File.ReadAllText(_IndexFilePath);
                    _FileIndex = Newtonsoft.Json.JsonConvert.DeserializeObject<List<FileIndexEntry>>(sJson) ?? new List<FileIndexEntry>();
                    if (_FileIndex.Count > _MaxIndexEntries ||
                        (DateTime.UtcNow - _LastIndexRebuild) > TimeSpan.FromHours(_IndexRebuildHours))
                    {
                        LogError($"Index needs rebuild. Entries: {_FileIndex.Count}, Last rebuild: {_LastIndexRebuild}");
                        RebuildIndex();
                    }
                    else
                    {
                        LogError($"Index loaded successfully. Entries: {_FileIndex.Count}");
                    }
                }
                else
                {
                    LogError("Index file not found. Rebuilding from existing files.");
                    RebuildIndex();
                }
            }
            catch (Exception ex)
            {
                LogError($"Failed to load index: {ex}. Rebuilding...");
                RebuildIndex();
            }
        }
    }
    private void RebuildIndex()
    {
        lock (_IndexLock)
        {
            try
            {
                _FileIndex.Clear();
                var oDirInfo = new DirectoryInfo(_StoragePath);
                foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.dat"))
                {
                    string sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                    if (Guid.TryParseExact(sFileName, "N", out Guid gGuid))
                    {
                        DateTime dtCreated = GetFileCreationTime(oFileInfo);
                        _FileIndex.Add(new FileIndexEntry { sId = gGuid.ToString("N"), dtCreated = dtCreated });
                    }
                }
                SaveIndex();
                _LastIndexRebuild = DateTime.UtcNow;
                LogError($"Index rebuilt successfully. Entries: {_FileIndex.Count}");
            }
            catch (Exception ex)
            {
                LogError($"Failed to rebuild index: {ex}");
            }
        }
    }
    //private void SaveIndex()
    //{
    //    lock (_oIndexLock)
    //    {
    //        const int iMaxRetries = 3;
    //        const int iBaseDelayMs = 100;
    //        string sTempPath = Path.Combine(_sStoragePath, "FileIndex.tmp");
    //        for (int iAttempt = 1; iAttempt <= iMaxRetries; iAttempt++)
    //        {
    //            try
    //            {
    //                if (File.Exists(sTempPath))
    //                {
    //                    try { File.Delete(sTempPath); }
    //                    catch { }
    //                }
    //                string sJson = Newtonsoft.Json.JsonConvert.SerializeObject(_lstFileIndex, Newtonsoft.Json.Formatting.Indented);
    //                File.WriteAllText(sTempPath, sJson);
    //                File.Replace(sTempPath, _sIndexFilePath, null);
    //                return;
    //            }
    //            catch (Exception ex) when (iAttempt < iMaxRetries)
    //            {
    //                LogError($"Attempt {iAttempt} to save index failed: {ex.Message}. Retrying...");
    //                Thread.Sleep(iBaseDelayMs * iAttempt);
    //            }
    //            catch (Exception ex)
    //            {
    //                LogError($"Failed to save index after {iMaxRetries} attempts: {ex}");
    //                try
    //                {
    //                    string sBackupPath = Path.Combine(_sStoragePath, "FileIndex_backup.json");
    //                    string sJson = Newtonsoft.Json.JsonConvert.SerializeObject(_lstFileIndex, Newtonsoft.Json.Formatting.Indented);
    //                    File.WriteAllText(sBackupPath, sJson);
    //                    if (File.Exists(_sIndexFilePath))
    //                    {
    //                        File.Delete(_sIndexFilePath);
    //                    }
    //                    File.Move(sBackupPath, _sIndexFilePath);
    //                    LogError("Index saved using fallback method");
    //                    return;
    //                }
    //                catch (Exception fallbackEx)
    //                {
    //                    LogError($"Fallback save also failed: {fallbackEx}");
    //                }
    //            }
    //            finally
    //            {
    //                if (File.Exists(sTempPath))
    //                {
    //                    try { File.Delete(sTempPath); }
    //                    catch { }
    //                }
    //            }
    //        }
    //    }
    //}
    private void SaveIndex()
    {
        lock (_IndexLock)
        {
            const int iMaxRetries = 3;
            const int iBaseDelayMs = 100;
            string sTempPath = Path.Combine(_StoragePath, "FileIndex.tmp");
            for (int iAttempt = 1; iAttempt <= iMaxRetries; iAttempt++)
            {
                try
                {
                    if (File.Exists(sTempPath))
                    {
                        try { File.Delete(sTempPath); }
                        catch { }
                    }
                    string sJson = Newtonsoft.Json.JsonConvert.SerializeObject(_FileIndex, Newtonsoft.Json.Formatting.Indented);
                    File.WriteAllText(sTempPath, sJson);

                    // بررسی وجود فایل مقصد قبل از جایگزینی
                    if (File.Exists(_IndexFilePath))
                    {
                        File.Replace(sTempPath, _IndexFilePath, null);
                    }
                    else
                    {
                        // اگر فایل مقصد وجود ندارد، از Move استفاده کن
                        File.Move(sTempPath, _IndexFilePath);
                    }

                    return;
                }
                catch (Exception ex) when (iAttempt < iMaxRetries)
                {
                    LogError($"Attempt {iAttempt} to save index failed: {ex.Message}. Retrying...");
                    Thread.Sleep(iBaseDelayMs * iAttempt);
                }
                catch (Exception ex)
                {
                    LogError($"Failed to save index after {iMaxRetries} attempts: {ex}");
                    try
                    {
                        string sBackupPath = Path.Combine(_StoragePath, "FileIndex_backup.json");
                        string sJson = Newtonsoft.Json.JsonConvert.SerializeObject(_FileIndex, Newtonsoft.Json.Formatting.Indented);
                        File.WriteAllText(sBackupPath, sJson);

                        // بررسی وجود فایل مقصد قبل از جایگزینی
                        if (File.Exists(_IndexFilePath))
                        {
                            File.Delete(_IndexFilePath);
                        }
                        File.Move(sBackupPath, _IndexFilePath);

                        LogError("Index saved using fallback method");
                        return;
                    }
                    catch (Exception fallbackEx)
                    {
                        LogError($"Fallback save also failed: {fallbackEx}");
                    }
                }
                finally
                {
                    if (File.Exists(sTempPath))
                    {
                        try { File.Delete(sTempPath); }
                        catch { }
                    }
                }
            }
        }
    }
    private void AddToIndex(Guid gFileId, DateTime dtCreated)
    {
        lock (_IndexLock)
        {
            _FileIndex.RemoveAll(e => e.sId == gFileId.ToString("N"));
            _FileIndex.Add(new FileIndexEntry { sId = gFileId.ToString("N"), dtCreated = dtCreated });
            SaveIndex();
        }
    }
    private void RemoveFromIndex(Guid gFileId)
    {
        lock (_IndexLock)
        {
            _FileIndex.RemoveAll(e => e.sId == gFileId.ToString("N"));
            SaveIndex();
        }
    }
    private void RemoveStaleEntries()
    {
        lock (_IndexLock)
        {
            var lstEntriesToRemove = new List<FileIndexEntry>();
            foreach (var oEntry in _FileIndex)
            {
                string sFilePath = Path.Combine(_StoragePath, oEntry.sId + ".dat");
                if (!File.Exists(sFilePath))
                {
                    lstEntriesToRemove.Add(oEntry);
                }
            }
            foreach (var oEntry in lstEntriesToRemove)
            {
                _FileIndex.Remove(oEntry);
            }
            if (lstEntriesToRemove.Count > 0)
            {
                SaveIndex();
                LogError($"Removed {lstEntriesToRemove.Count} stale entries from index");
            }
        }
    }
    private void CleanupTempIndexFiles()
    {
        try
        {
            var oDirInfo = new DirectoryInfo(_StoragePath);
            var sTempFiles = new[] { "FileIndex.tmp", "FileIndex_backup.json" };
            foreach (var sPattern in sTempFiles)
            {
                var oFiles = oDirInfo.GetFiles(sPattern);
                foreach (var oFile in oFiles)
                {
                    try
                    {
                        if (DateTime.UtcNow - oFile.CreationTimeUtc > TimeSpan.FromHours(1))
                        {
                            oFile.Delete();
                            LogError($"Deleted old temp file: {oFile.Name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogError($"Failed to delete temp file {oFile.Name}: {ex}");
                    }
                }
            }
            foreach (var oFile in oDirInfo.GetFiles("FileIndex_*.tmp"))
            {
                try
                {
                    if (DateTime.UtcNow - oFile.CreationTimeUtc > TimeSpan.FromHours(1))
                    {
                        oFile.Delete();
                        LogError($"Deleted old random temp file: {oFile.Name}");
                    }
                }
                catch (Exception ex)
                {
                    LogError($"Failed to delete random temp file {oFile.Name}: {ex}");
                }
            }
        }
        catch (Exception ex)
        {
            LogError($"Failed to cleanup temp files: {ex}");
        }
    }
    private void CheckPermissions()
    {
        try
        {
            string sTestFile = Path.Combine(_StoragePath, "permission_test.tmp");
            File.WriteAllText(sTestFile, "test");
            File.Delete(sTestFile);
            LogError("Permission check passed successfully");
        }
        catch (Exception ex)
        {
            string sCurrentUser = WindowsIdentity.GetCurrent().Name;
            throw new Exception(
                $"The application does not have write/delete permissions on the storage path: {_StoragePath}. " +
                $"Current user: {sCurrentUser}. " +
                $"Please grant 'Modify' permissions to the Application Pool identity. Error: {ex.Message}", ex);
        }
    }
    public Guid SaveFile(Stream oFileStream) => SaveFileCore(oFileStream);
    public Guid SaveFile(byte[] oFileData) => SaveFileCore(new MemoryStream(oFileData), oFileData.Length);
    public Guid SaveFile(string sBase64) => SaveFile(Convert.FromBase64String(sBase64));
    public Task<Guid> SaveFileAsync(Stream oFileStream, CancellationToken oCt = default) => SaveFileCoreAsync(oFileStream, null, oCt);
    public Task<Guid> SaveFileAsync(byte[] oFileData, CancellationToken oCt = default) => SaveFileCoreAsync(new MemoryStream(oFileData), oFileData.Length, oCt);
    public Task<Guid> SaveFileAsync(string sBase64, CancellationToken oCt = default) => SaveFileAsync(Convert.FromBase64String(sBase64), oCt);
    public Stream GetFile(Guid gFileId)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(gFileId);
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
    public async Task<Stream> GetFileAsync(Guid gFileId, CancellationToken oCt = default)
    {
        CheckDisposed();
        oCt.ThrowIfCancellationRequested();
        var sFilePath = GetFilePath(gFileId);
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
    public byte[] GetFileBytes(Guid gFileId)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(gFileId);
        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);
        var lFileSize = new FileInfo(sFilePath).Length;
        if (lFileSize > _MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes).");
        IncrementRef(gFileId);
        try
        {
            return ExecuteWithRetry(() =>
            {
                int iBufferSize = GetOptimizedBufferSize(lFileSize);
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
            DecrementRef(gFileId);
        }
    }
    public async Task<byte[]> GetFileBytesAsync(Guid gFileId, CancellationToken oCt = default)
    {
        CheckDisposed();
        var sFilePath = GetFilePath(gFileId);
        if (!File.Exists(sFilePath))
            throw new FileNotFoundException("File not found.", sFilePath);
        var lFileSize = new FileInfo(sFilePath).Length;
        if (lFileSize > _MaxFileSize)
            throw new IOException($"File is too large to load into memory (max {_MaxFileSize} bytes).");
        IncrementRef(gFileId);
        try
        {
            return await ExecuteWithRetryAsync(async () =>
            {
                int iBufferSize = GetOptimizedBufferSize(lFileSize);
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
            DecrementRef(gFileId);
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
        var gFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(gFileId);
        var sFinalPath = GetFilePath(gFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue ? GetOptimizedBufferSize(lKnownLength.Value) : _DefaultBufferSize;
        IncrementRef(gFileId);
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
                AddToIndex(gFileId, DateTime.UtcNow);
            });
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            DecrementRef(gFileId);
            throw;
        }
        return gFileId;
    }
    private async Task<Guid> SaveFileCoreAsync(Stream oStream, long? lKnownLength, CancellationToken oCt = default)
    {
        CheckDisposed();
        if (oStream == null)
            throw new ArgumentNullException(nameof(oStream));
        long lFileSize = lKnownLength ?? (oStream.CanSeek ? oStream.Length : _MaxFileSize);
        EnsureDiskSpace(lFileSize);
        var gFileId = Guid.NewGuid();
        var sTempPath = GetTempPath(gFileId);
        var sFinalPath = GetFilePath(gFileId);
        int iBufferSize = oStream.CanSeek && lKnownLength.HasValue
            ? GetOptimizedBufferSize(lKnownLength.Value)
            : _DefaultBufferSize;
        IncrementRef(gFileId);
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
                AddToIndex(gFileId, DateTime.UtcNow);
            }, oCt).ConfigureAwait(false);
        }
        catch
        {
            SafeDeleteFile(sTempPath);
            DecrementRef(gFileId);
            throw;
        }
        return gFileId;
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
            LogError("Cleanup timer triggered");
            CleanupTempIndexFiles();
            CleanupOldFiles();
        }
        catch (Exception ex)
        {
            LogError($"Cleanup failed: {ex}");
        }
        finally
        {
            Interlocked.Exchange(ref _CleanupRunning, 0);
            _CleanupCompleted.Set();
            if (_Disposed == 0)
            {
                int iMs = (int)Math.Min(TimeSpan.FromHours(_DeleteEveryHours).TotalMilliseconds, int.MaxValue);
                _CleanupTimer?.Change(iMs, iMs);
                LogError($"Cleanup timer reset for next run in {_DeleteEveryHours} hours");
            }
        }
    }
    private void CleanupOldFiles()
    {
        try
        {
            var dtCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
            LogError($"Cleanup started. Cutoff time (UTC): {dtCutoff}");
            RemoveStaleEntries();
            if ((DateTime.UtcNow - _LastIndexRebuild) > TimeSpan.FromHours(_IndexRebuildHours))
            {
                LogError("Scheduled index rebuild");
                RebuildIndex();
            }
            var lstEntriesToDelete = new List<FileIndexEntry>();
            lock (_IndexLock)
            {
                foreach (var oEntry in _FileIndex)
                {
                    if (oEntry.dtCreated < dtCutoff)
                    {
                        if (Guid.TryParseExact(oEntry.sId, "N", out Guid gFileId))
                        {
                            bool bHasActiveRef;
                            lock (_RefsLock)
                            {
                                bHasActiveRef = _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
                            }
                            if (!bHasActiveRef)
                            {
                                lstEntriesToDelete.Add(oEntry);
                            }
                        }
                    }
                }
            }
            int iDeletedCount = 0;
            int iFailedCount = 0;
            foreach (var oEntry in lstEntriesToDelete)
            {
                string sFilePath = Path.Combine(_StoragePath, oEntry.sId + ".dat");
                if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                {
                    iDeletedCount++;
                    LogError($"Deleted old file: {oEntry.sId}");
                }
                else
                {
                    iFailedCount++;
                    LogError($"Failed to delete file: {oEntry.sId}");
                }
            }
            var oDirInfo = new DirectoryInfo(_StoragePath);
            foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.tmp"))
            {
                if (oFileInfo.Name.Equals("FileIndex.tmp", StringComparison.OrdinalIgnoreCase))
                    continue;
                if (SafeDeleteFile(oFileInfo.FullName))
                {
                    iDeletedCount++;
                    LogError($"Deleted temp file: {oFileInfo.Name}");
                }
                else
                {
                    iFailedCount++;
                }
            }
            LogError($"Cleanup completed. Deleted: {iDeletedCount}, Failed: {iFailedCount}");
        }
        catch (Exception ex)
        {
            LogError($"Global cleanup error: {ex}");
        }
    }
    private bool DeleteFileAndRemoveFromIndex(string sFilePath, FileIndexEntry oEntry)
    {
        if (SafeDeleteFile(sFilePath))
        {
            lock (_IndexLock)
            {
                _FileIndex.Remove(oEntry);
                SaveIndex();
            }
            return true;
        }
        return false;
    }
    private DateTime GetFileCreationTime(FileInfo oFileInfo)
    {
        try
        {
            return oFileInfo.CreationTimeUtc;
        }
        catch
        {
            try
            {
                return oFileInfo.CreationTime.ToUniversalTime();
            }
            catch
            {
                return oFileInfo.LastWriteTimeUtc;
            }
        }
    }
    private string GetFilePath(Guid gFileId) => Path.Combine(_StoragePath, gFileId.ToString("N") + ".dat");
    private string GetTempPath(Guid gFileId) => Path.Combine(_StoragePath, gFileId.ToString("N") + ".tmp");
    private void IncrementRef(Guid gFileId)
    {
        lock (_RefsLock)
        {
            _ActiveRefs.AddOrUpdate(gFileId, 1, (key, value) => value + 1);
        }
    }
    private void DecrementRef(Guid gFileId)
    {
        lock (_RefsLock)
        {
            int iNewValue = _ActiveRefs.AddOrUpdate(gFileId, 0, (key, value) => value - 1);
            if (iNewValue <= 0)
            {
                _ActiveRefs.TryRemove(gFileId, out _);
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
                var oFileInfo = new FileInfo(sPath);
                if (oFileInfo.IsReadOnly)
                {
                    oFileInfo.IsReadOnly = false;
                    LogError($"Removed read-only attribute from file: {sPath}");
                }
                File.Delete(sPath);
                LogError($"Successfully deleted file: {sPath}");
                return true;
            }
            catch (Exception ex) when (ex is FileNotFoundException || ex is DirectoryNotFoundException)
            {
                return true;
            }
            catch (Exception ex) when (i < iMaxRetries - 1 && (ex is IOException || ex is UnauthorizedAccessException))
            {
                LogError($"Attempt {i + 1} to delete file '{sPath}' failed: {ex.Message}. Retrying in {iBaseDelayMs * (i + 1)}ms...");
                Thread.Sleep(iBaseDelayMs * (i + 1));
            }
            catch (Exception ex)
            {
                LogError($"Critical error deleting file '{sPath}': {ex}");
                return false;
            }
        }
        LogError($"Failed to delete file '{sPath}' after {iMaxRetries} attempts.");
        return false;
    }
    private void CheckDisposed()
    {
        if (_Disposed != 0)
            throw new ObjectDisposedException("FileStorage");
    }
    private bool IsTransientFileError(Exception oEx)
    {
        return oEx is IOException || oEx is UnauthorizedAccessException || oEx is SecurityException;
    }
    private void LogError(string sMessage)
    {
        try
        {
            string sLogEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {sMessage}{Environment.NewLine}";
            File.AppendAllText(_LogFilePath, sLogEntry);
        }
        catch { }
    }
    public void ForceCleanup()
    {
        OnCleanupTimer(null);
    }
    public void ForceCleanupOldFiles(int iHoursAgo = 24)
    {
        var dtCutoff = DateTime.UtcNow - TimeSpan.FromHours(iHoursAgo);
        LogError($"Forced cleanup. Cutoff time (UTC): {dtCutoff}");
        var lstEntriesToDelete = new List<FileIndexEntry>();
        lock (_IndexLock)
        {
            foreach (var oEntry in _FileIndex)
            {
                if (oEntry.dtCreated < dtCutoff)
                {
                    if (Guid.TryParseExact(oEntry.sId, "N", out Guid gFileId))
                    {
                        bool bHasActiveRef;
                        lock (_RefsLock)
                        {
                            bHasActiveRef = _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
                        }
                        if (!bHasActiveRef)
                        {
                            lstEntriesToDelete.Add(oEntry);
                        }
                    }
                }
            }
        }
        foreach (var oEntry in lstEntriesToDelete)
        {
            string sFilePath = Path.Combine(_StoragePath, oEntry.sId + ".dat");
            if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
            {
                LogError($"Force deleted: {oEntry.sId}");
            }
            else
            {
                LogError($"Failed to force delete: {oEntry.sId}");
            }
        }
    }
    public void RebuildIndexManually()
    {
        LogError("Manual index rebuild requested");
        RebuildIndex();
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
            LogError($"Error during timer disposal: {ex}");
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
                LogError($"Error disposing timer: {ex}");
            }
            _ActiveRefs.Clear();
            _CleanupCompleted.Dispose();
        }
    }
#if WEB
    void IRegisteredObject.Stop(bool bImmediate)
    {
        if (bImmediate)
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
        private readonly ConditionalFileStorage _oStorage;
        public FileStorageRegisteredObject(ConditionalFileStorage oStorage)
        {
            _oStorage = oStorage;
        }
        public void Stop(bool bImmediate)
        {
            if (bImmediate)
            {
                _oStorage.Dispose();
            }
            else
            {
                try
                {
                    _oStorage._CleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                    if (_oStorage._CleanupRunning != 0)
                    {
                        _oStorage._CleanupCompleted.Wait(TimeSpan.FromSeconds(10));
                    }
                }
                catch (Exception ex)
                {
                    _oStorage.LogError($"Error during graceful shutdown: {ex}");
                }
                finally
                {
                    _oStorage.Dispose();
                }
            }
        }
    }
#endif
}