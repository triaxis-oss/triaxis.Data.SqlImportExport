
using System.Buffers;

namespace triaxis.Data.SqlImportExport;

public class CsvWriter : IAsyncDisposable
{
    private readonly TextWriter _writer;
    private IEnumerable<string>? _header;
    private readonly Func<object?, string?> _formatter;
    private int _itemIndex;

    private const char _separator = ',';
    private static readonly SearchValues<char> _quoteIf = SearchValues.Create(['"', _separator, '\r', '\n']);

    /// <summary>
    /// Default value formatter, formats values to SQL-style string representations
    /// </summary>
    public static string? DefaultFormatter(object? item)
    {
        if (item is null || item == DBNull.Value) { return null; }
        if (item is string s) { return s; }
        if (item is DateTime dt) { return dt.ToString("yyyy-MM-dd HH:mm:ss"); }
        if (item is bool b) { return b ? "1" : "0"; }
        if (item is byte[] bytes) { return $"0x{Convert.ToHexString(bytes)}"; }
        return item.ToString() ?? "";
    }

    public CsvWriter(string fileName, IEnumerable<string> header, Func<object?, string?>? formatter = null)
        : this(new StreamWriter(fileName), header, formatter)
    {
    }

    public CsvWriter(TextWriter writer, IEnumerable<string> header, Func<object?, string?>? formatter = null)
    {
        _writer = writer;
        _header = header;
        _formatter = formatter ?? DefaultFormatter;
    }

    #region Public Methods

    public async ValueTask WriteRecordAsync(IEnumerable<object?> record)
    {
        await RequireHeaderAsync();
        await WriteRecordImplAsync(record);
    }

    public async ValueTask FlushAsync()
    {
        await RequireHeaderAsync();
        await _writer.FlushAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await FlushAsync();
        await _writer.DisposeAsync();
    }

    #endregion

    #region Implementation

    private async ValueTask RequireHeaderAsync()
    {
        if (_header is not null)
        {
            await WriteRecordImplAsync(_header);
            _header = null;
        }
    }

    private async ValueTask WriteRecordImplAsync(IEnumerable<object?> row)
    {
        foreach (var item in row)
        {
            await WriteFieldImplAsync(_formatter(item));
        }
        await EndRecordImplAsync();
    }

    private async ValueTask WriteFieldImplAsync(string? value)
    {
        // TODO: formatting
        if (_itemIndex > 0)
        {
            await _writer.WriteAsync(_separator);
        }
        if (value is null)
        {
            // only null becomes an empty field
            return;
        }
        if (value == "" || value.AsSpan().ContainsAny(_quoteIf))
        {
            // quote the field
            await _writer.WriteAsync('"');
            int i = 0;
            for (; ; )
            {
                int e = value.IndexOf('"', i);
                if (e < 0)
                {
                    await _writer.WriteAsync(value.AsMemory(i));
                    break;
                }
                await _writer.WriteAsync(value.AsMemory(i, e - i));
                await _writer.WriteAsync("\"\"");
                i = e + 1;
            }
            await _writer.WriteAsync('"');
        }
        else
        {
            await _writer.WriteAsync(value);
        }
        _itemIndex++;
    }

    private async ValueTask EndRecordImplAsync()
    {
        await _writer.WriteLineAsync();
        _itemIndex = 0;
    }

    #endregion
}
