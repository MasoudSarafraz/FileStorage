# FileStorage.cs

A high-performance, fault-tolerant helper for **transient file storage** on disk.  
Designed for concurrent workloads where speed matters and **temporary files older than one hour** are safe to delete.

---

## Key Highlights

| Feature | Benefit |
|---------|---------|
| **Thread-safe** | Supports thousands of concurrent readers & writers from any number of tasks/threads. |
| **High throughput** | Uses `FileOptions.WriteThrough`, `RandomAccess`, and carefully tuned buffer sizes. |
| **Fault-tolerant** | Exponential back-off retry (up to 3×) for every I/O operation, plus defensive exception handling in background cleanup. |
| **Zero-copy reads** | Returns a `byte[]` without extra allocations. |
| **Self-cleaning** | Automatic lazy cleanup removes stale files; manual `ForceCleanup()` available on demand. |
| **Zero external dependencies** | Pure .NET Standard—no packages, no config files. |

---

## Quick Start

```csharp
var storage = new FileStorage("/var/tmp/myApp");

// Save
Guid id = storage.SaveFile(File.ReadAllBytes("avatar.jpg"));

// Load
byte[] data = storage.GetFile(id);

// Optional: trigger cleanup right now
storage.ForceCleanup();
