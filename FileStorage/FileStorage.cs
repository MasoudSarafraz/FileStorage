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
using System.Globalization;
using Newtonsoft.Json;
using System.Linq;
#if WEB
using System.Web.Hosting;
#endif
public sealed class FileStorage : IDisposable
#if WEB
    , IRegisteredObject
#endif
{
    private readonly string _StoragePath;
    private readonly uint _DeleteEveryHours;
    private readonly ConcurrentDictionary<Guid, int> _ActiveRefs = new ConcurrentDictionary<Guid, int>();
    private Timer _oCleanupTimer;
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
    private readonly JsonSerializerSettings _JsonSettings;
    private readonly uint _MaxActiveRefHours;//حداکثر مهلت به یک فایل برای حذف شدن
#if WEB
    private FileStorageRegisteredObject _oRegisteredObject;
#endif
    private class FileIndexEntry
    {
        public string FileId { get; set; }
        public DateTime CreateDate { get; set; }
    }
    private class ReferenceCountedFileStream : FileStream
    {
        private readonly FileStorage _oStorage;
        private readonly Guid _gFileId;
        private bool _bDisposed;
        public ReferenceCountedFileStream(FileStorage oStorage, Guid gFileId, string sPath, FileMode oMode, FileAccess oAccess, FileShare oShare, int iBufferSize, FileOptions oOptions)
            : base(sPath, oMode, oAccess, oShare, iBufferSize, oOptions)
        {
            _oStorage = oStorage;
            _gFileId = gFileId;
            _oStorage.IncrementRef(_gFileId);
        }
        protected override void Dispose(bool bDisposing)
        {
            if (!_bDisposed)
            {
                if (bDisposing)
                {
                    _oStorage.DecrementRef(_gFileId);
                }
                _bDisposed = true;
            }
            base.Dispose(bDisposing);
        }
    }
    public FileStorage(string sPath, uint iDeleteEveryHours = 1)
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
        _MaxActiveRefHours = (uint)(iDeleteEveryHours * 2);
        Directory.CreateDirectory(_StoragePath);
        _DriveInfo = new DriveInfo(Path.GetPathRoot(_StoragePath));
        _LogFilePath = Path.Combine(_StoragePath, "FileStorageLogs.log");
        _IndexFilePath = Path.Combine(_StoragePath, "FileIndex.json");
        _JsonSettings = new JsonSerializerSettings
        {
            Culture = CultureInfo.InvariantCulture,
            DateFormatString = "yyyy-MM-ddTHH:mm:ss.fffffffZ",
            Formatting = Formatting.Indented
        };
        CheckPermissions();
        CleanupTempIndexFiles();
        LoadOrRebuildIndex();
#if WEB
        if (HostingEnvironment.IsHosted)
        {
            _oRegisteredObject = new FileStorageRegisteredObject(this);
            HostingEnvironment.RegisterObject(_oRegisteredObject);
        }
#endif
        var oDelay = TimeSpan.FromHours(iDeleteEveryHours);
        int iMs = (int)Math.Min((long)oDelay.TotalMilliseconds, int.MaxValue);
        _oCleanupTimer = new Timer(OnCleanupTimer, null, 30000, iMs);
        LogMessage($"FileStorage initialized. Path: {_StoragePath}, Cleanup interval: {iDeleteEveryHours} hours");
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
                    _FileIndex = JsonConvert.DeserializeObject<List<FileIndexEntry>>(sJson, _JsonSettings) ?? new List<FileIndexEntry>();
                    if (_FileIndex.Count > _MaxIndexEntries ||
                        (DateTime.UtcNow - _LastIndexRebuild) > TimeSpan.FromHours(_IndexRebuildHours))
                    {
                        LogMessage($"Index needs rebuild. Entries: {_FileIndex.Count}, Last rebuild: {_LastIndexRebuild}");
                        RebuildIndex();
                    }
                    else
                    {
                        LogMessage($"Index loaded successfully. Entries: {_FileIndex.Count}");
                    }
                }
                else
                {
                    LogMessage("Index file not found. Rebuilding from existing files.");
                    RebuildIndex();
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to load index: {ex}. Rebuilding...");
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
                        _FileIndex.Add(new FileIndexEntry { FileId = gGuid.ToString("N"), CreateDate = dtCreated });
                    }
                }
                SaveIndex();
                _LastIndexRebuild = DateTime.UtcNow;
                LogMessage($"Index rebuilt successfully. Entries: {_FileIndex.Count}");
            }
            catch (Exception ex)
            {
                LogMessage($"Failed to rebuild index: {ex}");
            }
        }
    }
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
                    string sJson = JsonConvert.SerializeObject(_FileIndex, _JsonSettings);
                    File.WriteAllText(sTempPath, sJson, Encoding.UTF8);
                    if (File.Exists(_IndexFilePath))
                    {
                        File.Replace(sTempPath, _IndexFilePath, null);
                    }
                    else
                    {
                        File.Move(sTempPath, _IndexFilePath);
                    }
                    return;
                }
                catch (Exception ex) when (iAttempt < iMaxRetries)
                {
                    LogMessage($"Attempt {iAttempt} to save index failed: {ex.Message}. Retrying...");
                    Thread.Sleep(iBaseDelayMs * iAttempt);
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to save index after {iMaxRetries} attempts: {ex}");
                    try
                    {
                        string sBackupPath = Path.Combine(_StoragePath, "FileIndex_backup.json");
                        string sJson = JsonConvert.SerializeObject(_FileIndex, _JsonSettings);
                        File.WriteAllText(sBackupPath, sJson, Encoding.UTF8);
                        if (File.Exists(_IndexFilePath))
                        {
                            File.Delete(_IndexFilePath);
                        }
                        File.Move(sBackupPath, _IndexFilePath);
                        LogMessage("Index saved using fallback method");
                        return;
                    }
                    catch (Exception fallbackEx)
                    {
                        LogMessage($"Fallback save also failed: {fallbackEx}");
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
            dtCreated = dtCreated.ToUniversalTime();
            _FileIndex.RemoveAll(e => e.FileId == gFileId.ToString("N"));
            _FileIndex.Add(new FileIndexEntry { FileId = gFileId.ToString("N"), CreateDate = dtCreated });
            SaveIndex();
        }
    }
    private void RemoveFromIndex(Guid gFileId)
    {
        lock (_IndexLock)
        {
            _FileIndex.RemoveAll(e => e.FileId == gFileId.ToString("N"));
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
                string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
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
                LogMessage($"Removed {lstEntriesToRemove.Count} stale entries from index");
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
                            LogMessage($"Deleted old temp file: {oFile.Name}");
                        }
                    }
                    catch (Exception ex)
                    {
                        LogMessage($"Failed to delete temp file {oFile.Name}: {ex}");
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
                        LogMessage($"Deleted old random temp file: {oFile.Name}");
                    }
                }
                catch (Exception ex)
                {
                    LogMessage($"Failed to delete random temp file {oFile.Name}: {ex}");
                }
            }
        }
        catch (Exception ex)
        {
            LogMessage($"Failed to cleanup temp files: {ex}");
        }
    }
    private void CheckPermissions()
    {
        try
        {
            string sTestFile = Path.Combine(_StoragePath, "permission_test.tmp");
            File.WriteAllText(sTestFile, "test");
            File.Delete(sTestFile);
            LogMessage("Permission check passed successfully");
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
            return new ReferenceCountedFileStream(
                this,
                gFileId,
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
            return new ReferenceCountedFileStream(
                this,
                gFileId,
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
            throw;
        }
        finally
        {
            DecrementRef(gFileId);
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
            throw;
        }
        finally
        {
            DecrementRef(gFileId);
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
            LogMessage("Cleanup timer triggered");
            CleanupTempIndexFiles();
            CleanupOrphanedReferences();
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
            if (_Disposed == 0)
            {
                int iMs = (int)Math.Min(TimeSpan.FromHours(_DeleteEveryHours).TotalMilliseconds, int.MaxValue);
                _oCleanupTimer?.Change(iMs, iMs);
                LogMessage($"Cleanup timer reset for next run in {_DeleteEveryHours} hours");
            }
        }
    }
    private void CleanupOldFiles()
    {
        try
        {
            LogMessage($"Current system time (UTC): {DateTime.UtcNow}");
            LogMessage($"Current system time (Local): {DateTime.Now}");
            var dtCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
            var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours + _MaxActiveRefHours);
            LogMessage($"Cleanup started. Cutoff time (UTC): {dtCutoff} (Local: {dtCutoff.ToLocalTime()})");
            LogMessage($"Force delete cutoff (UTC): {dtForceDeleteCutoff} (Local: {dtForceDeleteCutoff.ToLocalTime()})");

            RemoveStaleEntries();

            if ((DateTime.UtcNow - _LastIndexRebuild) > TimeSpan.FromHours(_IndexRebuildHours))
            {
                LogMessage("Scheduled index rebuild");
                RebuildIndex();
            }

            int iDeletedCount = 0;
            int iFailedCount = 0;

            var lstEntriesToDelete = new List<FileIndexEntry>();
            lock (_IndexLock)
            {
                LogMessage($"Processing {_FileIndex.Count} entries in index");
                foreach (var oEntry in _FileIndex)
                {
                    LogMessage($"Processing file: {oEntry.FileId}");
                    LogMessage($"  File created (UTC): {oEntry.CreateDate}");
                    LogMessage($"  File created (Local): {oEntry.CreateDate.ToLocalTime()}");
                    LogMessage($"  Cutoff time (UTC): {dtCutoff}");
                    LogMessage($"  Force delete cutoff (UTC): {dtForceDeleteCutoff}");

                    bool bIsOldEnough = oEntry.CreateDate < dtCutoff;
                    bool bIsVeryOld = oEntry.CreateDate < dtForceDeleteCutoff;
                    LogMessage($"  Is old enough? {bIsOldEnough}");
                    LogMessage($"  Is very old? {bIsVeryOld}");

                    if (bIsVeryOld)
                    {
                        lstEntriesToDelete.Add(oEntry);
                        LogMessage($"  FORCE DELETION (very old): {oEntry.FileId}");
                    }
                    else if (bIsOldEnough)
                    {
                        if (Guid.TryParseExact(oEntry.FileId, "N", out Guid gFileId))
                        {
                            bool bHasActiveRef;
                            lock (_RefsLock)
                            {
                                bHasActiveRef = _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
                            }
                            LogMessage($"  Has active reference: {bHasActiveRef}");

                            if (!bHasActiveRef)
                            {
                                lstEntriesToDelete.Add(oEntry);
                                LogMessage($"  MARKED FOR DELETION: {oEntry.FileId}");
                            }
                            else
                            {
                                LogMessage($"  NOT DELETED (has active ref but not old enough for force): {oEntry.FileId}");
                            }
                        }
                        else
                        {
                            LogMessage($"  NOT DELETED (invalid GUID): {oEntry.FileId}");
                        }
                    }
                    else
                    {
                        LogMessage($"  NOT DELETED (not old enough): {oEntry.FileId}");
                    }
                }
            }

            LogMessage($"Found {lstEntriesToDelete.Count} files to delete from index");
            foreach (var oEntry in lstEntriesToDelete)
            {
                string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                LogMessage($"Attempting to delete: {oEntry.FileId}");
                if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                {
                    iDeletedCount++;
                    LogMessage($"SUCCESSFULLY deleted: {oEntry.FileId}");
                }
                else
                {
                    iFailedCount++;
                    LogMessage($"FAILED to delete: {oEntry.FileId}");
                }
            }

            var oDirInfo = new DirectoryInfo(_StoragePath);
            var allFiles = oDirInfo.GetFiles("*.dat");
            LogMessage($"Found {allFiles.Length} .dat files in directory");

            foreach (var oFileInfo in allFiles)
            {
                string sFileName = Path.GetFileNameWithoutExtension(oFileInfo.Name);
                if (Guid.TryParseExact(sFileName, "N", out Guid gFileId))
                {
                    bool bInIndex = _FileIndex.Any(e => e.FileId == gFileId.ToString("N"));

                    if (!bInIndex)
                    {
                        DateTime dtCreated = GetFileCreationTime(oFileInfo);
                        bool bIsOldEnough = dtCreated < dtCutoff;
                        bool bIsVeryOld = dtCreated < dtForceDeleteCutoff;

                        LogMessage($"File not in index: {sFileName}");
                        LogMessage($"  Created (UTC): {dtCreated}");
                        LogMessage($"  Is old enough: {bIsOldEnough}");
                        LogMessage($"  Is very old: {bIsVeryOld}");

                        if (bIsVeryOld || bIsOldEnough)
                        {
                            if (SafeDeleteFile(oFileInfo.FullName))
                            {
                                iDeletedCount++;
                                LogMessage($"Deleted old file not in index: {sFileName}");
                            }
                            else
                            {
                                iFailedCount++;
                                LogMessage($"Failed to delete file not in index: {sFileName}");
                            }
                        }
                        else
                        {
                            LogMessage($"File not in index but not old enough: {sFileName}");
                        }
                    }
                }
            }

            foreach (var oFileInfo in oDirInfo.EnumerateFiles("*.tmp"))
            {
                if (oFileInfo.Name.Equals("FileIndex.tmp", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (SafeDeleteFile(oFileInfo.FullName))
                {
                    iDeletedCount++;
                    LogMessage($"Deleted temp file: {oFileInfo.Name}");
                }
                else
                {
                    iFailedCount++;
                }
            }

            LogMessage($"Cleanup completed. Deleted: {iDeletedCount}, Failed: {iFailedCount}");
        }
        catch (Exception ex)
        {
            LogMessage($"Global cleanup error: {ex}");
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
            var dtUtc = oFileInfo.CreationTimeUtc;
            LogMessage($"Got creation time from UTC: {dtUtc}");
            return dtUtc;
        }
        catch (Exception exUtc)
        {
            LogMessage($"Failed to get UTC creation time: {exUtc}");
            try
            {
                var dtLocal = oFileInfo.CreationTime;
                var dtConverted = dtLocal.ToUniversalTime();
                LogMessage($"Got creation time from Local: {dtLocal}, Converted to UTC: {dtConverted}");
                return dtConverted;
            }
            catch (Exception exLocal)
            {
                LogMessage($"Failed to get Local creation time: {exLocal}");
                try
                {
                    var dtWrite = oFileInfo.LastWriteTimeUtc;
                    LogMessage($"Falling back to LastWriteTimeUtc: {dtWrite}");
                    return dtWrite;
                }
                catch (Exception exWrite)
                {
                    LogMessage($"Failed to get LastWriteTimeUtc: {exWrite}");
                    return DateTime.UtcNow;
                }
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
                    LogMessage($"Removed read-only attribute from file: {sPath}");
                }
                File.Delete(sPath);
                LogMessage($"Successfully deleted file: {sPath}");
                return true;
            }
            catch (Exception ex) when (ex is FileNotFoundException || ex is DirectoryNotFoundException)
            {
                return true;
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
        if (_Disposed != 0)
            throw new ObjectDisposedException("FileStorage");
    }
    private bool IsTransientFileError(Exception oEx)
    {
        return oEx is IOException || oEx is UnauthorizedAccessException || oEx is SecurityException;
    }
    private void LogMessage(string sMessage)
    {
        try
        {
            string sLogEntry = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss}: {sMessage}{Environment.NewLine}";
            File.AppendAllText(_LogFilePath, sLogEntry, Encoding.UTF8);
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
        var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromHours(iHoursAgo + _MaxActiveRefHours);
        LogMessage($"Forced cleanup. Cutoff time (UTC): {dtCutoff}");
        LogMessage($"Force delete cutoff (UTC): {dtForceDeleteCutoff}");

        var lstEntriesToDelete = new List<FileIndexEntry>();
        lock (_IndexLock)
        {
            foreach (var oEntry in _FileIndex)
            {
                if (oEntry.CreateDate < dtForceDeleteCutoff)
                {
                    lstEntriesToDelete.Add(oEntry);
                }
                else if (oEntry.CreateDate < dtCutoff)
                {
                    if (Guid.TryParseExact(oEntry.FileId, "N", out Guid gFileId))
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
            string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
            if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
            {
                LogMessage($"Force deleted: {oEntry.FileId}");
            }
            else
            {
                LogMessage($"Failed to force delete: {oEntry.FileId}");
            }
        }
    }
    public void RebuildIndexManually()
    {
        LogMessage("Manual index rebuild requested");
        RebuildIndex();
    }
    public void TestFileDeletion(string sFileId)
    {
        var dtCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
        var dtForceDeleteCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours + _MaxActiveRefHours);
        lock (_IndexLock)
        {
            var oEntry = _FileIndex.FirstOrDefault(e => e.FileId == sFileId);
            if (oEntry != null)
            {
                LogMessage($"Testing file: {sFileId}");
                LogMessage($"  File created: {oEntry.CreateDate}");
                LogMessage($"  Cutoff time: {dtCutoff}");
                LogMessage($"  Force delete cutoff: {dtForceDeleteCutoff}");
                LogMessage($"  Should delete: {oEntry.CreateDate < dtCutoff}");
                LogMessage($"  Should force delete: {oEntry.CreateDate < dtForceDeleteCutoff}");

                if (oEntry.CreateDate < dtForceDeleteCutoff)
                {
                    string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                    if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                    {
                        LogMessage($"  MANUALLY FORCE DELETED: {sFileId}");
                    }
                }
                else if (oEntry.CreateDate < dtCutoff)
                {
                    if (Guid.TryParseExact(oEntry.FileId, "N", out Guid gFileId))
                    {
                        bool bHasActiveRef;
                        lock (_RefsLock)
                        {
                            bHasActiveRef = _ActiveRefs.TryGetValue(gFileId, out int iCount) && iCount > 0;
                        }
                        LogMessage($"  Has active reference: {bHasActiveRef}");
                        if (!bHasActiveRef)
                        {
                            string sFilePath = Path.Combine(_StoragePath, oEntry.FileId + ".dat");
                            if (DeleteFileAndRemoveFromIndex(sFilePath, oEntry))
                            {
                                LogMessage($"  MANUALLY DELETED: {sFileId}");
                            }
                        }
                    }
                }
            }
            else
            {
                LogMessage($"File not found in index: {sFileId}");
            }
        }
    }
    public void CleanupOrphanedReferences()
    {
        LogMessage("Cleaning up orphaned references");
        var lstOrphanedRefs = new List<Guid>();
        lock (_RefsLock)
        {
            foreach (var kvp in _ActiveRefs)
            {
                string sFilePath = Path.Combine(_StoragePath, kvp.Key.ToString("N") + ".dat");
                if (!File.Exists(sFilePath))
                {
                    lstOrphanedRefs.Add(kvp.Key);
                }
            }
            foreach (var gFileId in lstOrphanedRefs)
            {
                _ActiveRefs.TryRemove(gFileId, out _);
                LogMessage($"Removed orphaned reference: {gFileId}");
            }
        }
        LogMessage($"Cleaned up {lstOrphanedRefs.Count} orphaned references");
    }
    public void Dispose()
    {
        if (Interlocked.CompareExchange(ref _Disposed, 1, 0) != 0)
            return;
        try
        {
            _oCleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
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
                _oCleanupTimer?.Dispose();
#if WEB
                if (_oRegisteredObject != null)
                {
                    HostingEnvironment.UnregisterObject(_oRegisteredObject);
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
                _oCleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                if (_CleanupRunning != 0)
                {
                    _CleanupCompleted.Wait(TimeSpan.FromSeconds(10));
                }
            }
            catch (Exception ex)
            {
                LogMessage($"Error during graceful shutdown: {ex}");
            }
            finally
            {
                Dispose();
            }
        }
    }
    private class FileStorageRegisteredObject : IRegisteredObject
    {
        private readonly FileStorage _Storage;
        public FileStorageRegisteredObject(FileStorage oStorage)
        {
            _Storage = oStorage;
        }
        public void Stop(bool bImmediate)
        {
            if (bImmediate)
            {
                _Storage.Dispose();
            }
            else
            {
                try
                {
                    _Storage._oCleanupTimer?.Change(Timeout.Infinite, Timeout.Infinite);
                    if (_Storage._CleanupRunning != 0)
                    {
                        _Storage._CleanupCompleted.Wait(TimeSpan.FromSeconds(10));
                    }
                }
                catch (Exception ex)
                {
                    _Storage.LogMessage($"Error during graceful shutdown: {ex}");
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