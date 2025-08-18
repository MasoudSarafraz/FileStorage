using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public sealed class FileStorage : IDisposable
{
    private readonly string _StoragePath;
    private DateTime _LastCleanup = DateTime.UtcNow;
    private int _CleanupFlag;
    private readonly uint _DeleteEveryHours;
    private readonly ConcurrentDictionary<Guid, byte> _ActiveFiles = new ConcurrentDictionary<Guid, byte>();
    private bool _Disposed;

    public FileStorage(string sPath, uint iDeleteEveryHours = 1)
    {
        if (string.IsNullOrWhiteSpace(sPath))
            throw new ArgumentNullException(nameof(sPath));

        this._StoragePath = sPath;
        this._DeleteEveryHours = iDeleteEveryHours;
        Directory.CreateDirectory(sPath);
    }
    public Guid SaveFile(byte[] oFileData, string sBase64 = null)
    {
        if ((oFileData == null || oFileData.Length == 0) && string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");
        oFileData = oFileData ?? Convert.FromBase64String(sBase64);
        var oFileId = Guid.NewGuid();
        var sFilePath = GetFilePath(oFileId);

        try
        {
            _ActiveFiles[oFileId] = 0;
            ExecuteWithRetry(() =>
            {
                using (var oFs = new FileStream(
                    sFilePath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 81920,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    oFs.Write(oFileData, 0, oFileData.Length);
                }
            });
        }
        finally
        {
            _ActiveFiles.TryRemove(oFileId, out _);
        }

        TriggerLazyCleanup();
        return oFileId;
    }
    public async Task<Guid> SaveFileAsync(byte[] oFileData, string sBase64 = null)
    {
        if ((oFileData == null || oFileData.Length == 0) && string.IsNullOrEmpty(sBase64))
            throw new ArgumentException("File data is empty");
        oFileData = oFileData ?? Convert.FromBase64String(sBase64);
        var oFileId = Guid.NewGuid();
        var sFilePath = GetFilePath(oFileId);

        try
        {
            _ActiveFiles[oFileId] = 0;
            await ExecuteWithRetryAsync(async () =>
            {
                using (var oFs = new FileStream(
                    sFilePath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 81920,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    await oFs.WriteAsync(oFileData, 0, oFileData.Length);
                }
            });
        }
        finally
        {
            _ActiveFiles.TryRemove(oFileId, out _);
        }

        TriggerLazyCleanup();
        return oFileId;
    }
    public byte[] GetFile(Guid oFileId)
    {
        var sFilePath = GetFilePath(oFileId);

        try
        {
            _ActiveFiles[oFileId] = 0;
            return ExecuteWithRetry(() =>
            {
                using (var oFs = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    bufferSize: 81920,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    var oBuffer = new byte[oFs.Length];
                    int iRead = oFs.Read(oBuffer, 0, oBuffer.Length);
                    if (iRead != oBuffer.Length)
                        throw new IOException("File read incomplete");
                    return oBuffer;
                }
            });
        }
        finally
        {
            _ActiveFiles.TryRemove(oFileId, out _);
        }
    }
    public async Task<byte[]> GetFileAsync(Guid oFileId)
    {
        var sFilePath = GetFilePath(oFileId);

        try
        {
            _ActiveFiles[oFileId] = 0;
            return await ExecuteWithRetryAsync(async () =>
            {
                using (var oFs = new FileStream(
                    sFilePath,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read | FileShare.Delete,
                    bufferSize: 81920,
                    FileOptions.Asynchronous | FileOptions.SequentialScan))
                {
                    var oBuffer = new byte[oFs.Length];
                    int iRead = await oFs.ReadAsync(oBuffer, 0, oBuffer.Length);
                    if (iRead != oBuffer.Length)
                        throw new IOException("File read incomplete");
                    return oBuffer;
                }
            });
        }
        finally
        {
            _ActiveFiles.TryRemove(oFileId, out _);
        }
    }
    public void ForceCleanup()
    {
        if (Interlocked.CompareExchange(ref _CleanupFlag, 1, 0) != 0)
            return;

        Task.Run(() =>
        {
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
                Interlocked.Exchange(ref _CleanupFlag, 0);
            }
        });
    }
    private void TriggerLazyCleanup()
    {
        if ((DateTime.UtcNow - _LastCleanup) <= TimeSpan.FromHours(_DeleteEveryHours))
            return;

        ForceCleanup();
    }
    private void CleanupOldFiles()
    {
        _LastCleanup = DateTime.UtcNow;
        //var dCutoff = DateTime.UtcNow.AddHours(iDeleteEveryHours * -1);
        var dCutoff = DateTime.UtcNow - TimeSpan.FromHours(_DeleteEveryHours);
        try
        {
            foreach (var sFilePath in Directory.GetFiles(_StoragePath, "*.tmp"))
            {
                try
                {
                    var sFileName = Path.GetFileNameWithoutExtension(sFilePath);
                    if (Guid.TryParse(sFileName, out var oFileId) && _ActiveFiles.ContainsKey(oFileId))
                        continue;

                    var dCreationTime = File.GetCreationTimeUtc(sFilePath);
                    if (dCreationTime < dCutoff)
                        File.Delete(sFilePath);
                }
                catch (IOException) { }
                catch (UnauthorizedAccessException) { }
            }
        }
        catch (Exception ex)
        {
            LogError("Global cleanup error", ex);
        }
    }
    private string GetFilePath(Guid oFileId) =>
        Path.Combine(_StoragePath, oFileId.ToString("N") + ".tmp");
    private T ExecuteWithRetry<T>(Func<T> oFunc, int iMaxRetries = 5, int iBaseDelay = 20)
    {
        var rnd = new Random();
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                return oFunc();
            }
            catch (IOException) when (i < iMaxRetries - 1)
            {
                Thread.Sleep(iBaseDelay * (1 << i) + rnd.Next(5, 50));
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
    private async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> oFunc, int iMaxRetries = 5, int iBaseDelay = 20)
    {
        var rnd = new Random();
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                return await oFunc();
            }
            catch (IOException) when (i < iMaxRetries - 1)
            {
                await Task.Delay(iBaseDelay * (1 << i) + rnd.Next(5, 50));
            }
        }
        throw new IOException("Async file operation failed after retries");
    }
    private async Task ExecuteWithRetryAsync(Func<Task> oAction, int iMaxRetries = 5, int iBaseDelay = 20)
    {
        await ExecuteWithRetryAsync(async () =>
        {
            await oAction();
            return true;
        }, iMaxRetries, iBaseDelay);
    }
    private void LogError(string sMessage, Exception oEx)
    {
        //Console.Error.WriteLine($"{sMessage}: {oEx.Message}");
    }
    public void Dispose()
    {
        if (_Disposed) return;
        _Disposed = true;
        _ActiveFiles.Clear();
    }
}
