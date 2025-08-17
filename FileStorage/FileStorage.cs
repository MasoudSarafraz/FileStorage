using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

public class FileStorage
{
    private readonly string _StoragePath;
    private DateTime _LastCleanup = DateTime.UtcNow;
    private int _CleanupFlag;
    private uint iCuttOff;
    private readonly ConcurrentDictionary<Guid, byte> _ActiveFiles = new ConcurrentDictionary<Guid, byte>();

    public FileStorage(string sStoragePath, uint iEveryHours = 1)
    {
        iCuttOff = iEveryHours;
        if (string.IsNullOrWhiteSpace(sStoragePath))
            throw new ArgumentNullException(nameof(sStoragePath));

        this._StoragePath = sStoragePath;
        Directory.CreateDirectory(sStoragePath);
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
                    bufferSize: 4096,
                    FileOptions.WriteThrough | FileOptions.SequentialScan))
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
                    bufferSize: 4096,
                    FileOptions.RandomAccess | FileOptions.SequentialScan))
                {
                    var oBuffer = new byte[oFs.Length];
                    oFs.Read(oBuffer, 0, oBuffer.Length);
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
            finally
            {
                Interlocked.Exchange(ref _CleanupFlag, 0);
            }
        });
    }

    private void TriggerLazyCleanup()
    {
        if ((DateTime.UtcNow - _LastCleanup) <= TimeSpan.FromHours(iCuttOff))
            return;

        ForceCleanup();
    }

    private void CleanupOldFiles()
    {
        _LastCleanup = DateTime.UtcNow;
        var dCutoff = DateTime.UtcNow.AddHours(iCuttOff * -1);

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
                    {
                        File.Delete(sFilePath);
                    }
                }
                catch (IOException) { }
                catch (UnauthorizedAccessException) { }
            }
        }
        catch (Exception) { }
    }

    private string GetFilePath(Guid oFileId)
    {
        return Path.Combine(_StoragePath, oFileId.ToString("N") + ".tmp");
    }

    private T ExecuteWithRetry<T>(Func<T> oFunc, int iMaxRetries = 3, int iBaseDelay = 10)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                return oFunc();
            }
            catch (IOException) when (i < iMaxRetries - 1)
            {
                Thread.Sleep(iBaseDelay * (1 << i));
            }
        }
        throw new IOException("File operation failed after retries");
    }

    private void ExecuteWithRetry(Action oAction, int iMaxRetries = 3, int iBaseDelay = 10)
    {
        for (int i = 0; i < iMaxRetries; i++)
        {
            try
            {
                oAction();
                return;
            }
            catch (IOException) when (i < iMaxRetries - 1)
            {
                Thread.Sleep(iBaseDelay * (1 << i));
            }
        }
        throw new IOException("File operation failed after retries");
    }
}
