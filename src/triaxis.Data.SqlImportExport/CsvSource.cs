using System.Diagnostics;
using System.Globalization;

namespace triaxis.Data.SqlImportExport;

public class CsvSource : IBulkImportSource
{
    private readonly string _name;
    private readonly TextReader _reader;
    private string[]? _fields;
    private const char _separator = ',';

    public CsvSource(string name, TextReader reader)
    {
        _name = name;
        _reader = reader;
    }

    public string Name => _name;

    public static async IAsyncEnumerable<IBulkImportSource> FromDirectory(string path, string pattern = "*.csv")
    {
        foreach (var file in Directory.EnumerateFiles(path, "*.csv").OrderBy(f => Path.GetFileName(f)))
        {
            await using var fs = File.OpenRead(file);
            using var reader = new StreamReader(fs);
            yield return new CsvSource(Path.GetFileNameWithoutExtension(file), reader);
        }
    }

    public async Task<IEnumerable<string>> GetColumnNamesAsync()
        => await EnsureFieldsAsync();

    public async IAsyncEnumerable<object[]> EnumerateDataAsync()
    {
        while (await ReadRecordAsync() is { } record)
        {
            yield return record;
        }
    }

    private async ValueTask<string[]> EnsureFieldsAsync()
    {
        return _fields ??= await ReadHeaderAsync();
    }

    private async Task<string[]> ReadHeaderAsync()
    {
        var res = new List<string>();
        await foreach (var v in ParseLine(await _reader.ReadLineAsync() ?? throw new FormatException("Empty CSV file")))
        {
            res.Add(v.ToString() ?? "");
        }
        return res.ToArray();
    }

    private async ValueTask<object[]?> ReadRecordAsync()
    {
        var fields = await EnsureFieldsAsync();
        var line = await _reader.ReadLineAsync();
        if (line is null)
        {
            return null;
        }

        int i = 0;
        var res = new object[fields.Length];
        await foreach (var value in ParseLine(line))
        {
            res[i++] = value;
        }
        return res;
    }

    private async IAsyncEnumerable<object> ParseLine(string line)
    {
        int pos = 0;
        List<ReadOnlyMemory<char>>? multiline = null;
        while (pos < line.Length)
        {
            bool isQuoted = line[pos] == '"';
            bool hasEscapedQuotes = false;
            if (isQuoted)
            {
                int end = pos;
                for (; ; )
                {
                    end = line.IndexOf('"', end + 1);

                    if (end < 0)
                    {
                        if (multiline is null)
                        {
                            // skip the initial quote so it doesn't get included in the final value
                            multiline = [line.AsMemory(pos + 1)];
                        }
                        else
                        {
                            // subsequent lines are used completely
                            Debug.Assert(pos == 0);
                            multiline.Add(line.AsMemory());
                        }
                        if (await _reader.ReadLineAsync() is not { } nextLine)
                        {
                            throw new FormatException("Unexpected end of CSV file inside a multiline quoted field");
                        }
                        line = nextLine;
                        pos = 0;
                        end = -1;
                        continue;
                    }

                    if (end + 1 >= line.Length || line[end + 1] != '"')
                    {
                        break;
                    }
                    hasEscapedQuotes = true;
                    end++;
                }

                ReadOnlyMemory<char> mem;
                if (multiline is null)
                {
                    // common case - just part of a single line
                    mem = line.AsMemory(pos + 1, end - pos - 1);
                }
                else
                {
                    // combine all the parts together
                    Debug.Assert(pos == 0);
                    multiline.Add(line.AsMemory(0, end));
                    mem = string.Join('\n', multiline).AsMemory();
                    multiline = null;
                }

                yield return new CsvValue(mem, hasEscapedQuotes);
                pos = end + 1;
                if (pos < line.Length && line[pos] != _separator)
                {
                    throw new FormatException("Expected separator after quoted field");
                }
                pos++;
            }
            else
            {
                int end = line.IndexOf(_separator, pos);
                if (end < 0)
                {
                    end = line.Length;
                }
                if (end == pos)
                {
                    yield return DBNull.Value;
                }
                else
                {
                    yield return new CsvValue(line.AsMemory(pos, end - pos), false);
                }
                pos = end + 1;
            }
        }
    }

    class CsvValue : IConvertible
    {
        private static readonly CultureInfo ic = CultureInfo.InvariantCulture;

        private ReadOnlyMemory<char> _raw;
        private bool _hasDoubleQuotes;

        public CsvValue(ReadOnlyMemory<char> raw, bool hasDoubleQuotes)
        {
            _raw = raw;
            _hasDoubleQuotes = hasDoubleQuotes;
        }

        public TypeCode GetTypeCode() => TypeCode.String;

        public bool ToBoolean(IFormatProvider? provider) => int.TryParse(_raw.Span, provider ?? ic, out var n) ? n != 0 : bool.Parse(_raw.Span);
        public byte ToByte(IFormatProvider? provider) => byte.Parse(_raw.Span, provider ?? ic);
        public char ToChar(IFormatProvider? provider) => _raw.Span[0];
        public DateTime ToDateTime(IFormatProvider? provider) => DateTime.Parse(_raw.Span, provider ?? ic);
        public decimal ToDecimal(IFormatProvider? provider) => decimal.Parse(_raw.Span, provider ?? ic);
        public double ToDouble(IFormatProvider? provider) => double.Parse(_raw.Span, provider ?? ic);
        public short ToInt16(IFormatProvider? provider) => short.Parse(_raw.Span, provider ?? ic);
        public int ToInt32(IFormatProvider? provider) => int.Parse(_raw.Span, provider ?? ic);
        public long ToInt64(IFormatProvider? provider) => long.Parse(_raw.Span, provider ?? ic);
        public sbyte ToSByte(IFormatProvider? provider) => sbyte.Parse(_raw.Span, provider ?? ic);
        public float ToSingle(IFormatProvider? provider) => float.Parse(_raw.Span, provider ?? ic);
        public ushort ToUInt16(IFormatProvider? provider) => ushort.Parse(_raw.Span, provider ?? ic);
        public uint ToUInt32(IFormatProvider? provider) => uint.Parse(_raw.Span, provider ?? ic);
        public ulong ToUInt64(IFormatProvider? provider) => ulong.Parse(_raw.Span, provider ?? ic);

        public override string ToString() => ToString(null);
        public string ToString(IFormatProvider? provider)
        {
            if (_hasDoubleQuotes)
            {
                _hasDoubleQuotes = false;
                var span = _raw.Span;
                _raw = string.Create(span.Length - (span.Count('"') / 2), _raw, (res, mem) =>
                {
                    var src = mem.Span;
                    while (!src.IsEmpty)
                    {
                        int q = src.IndexOf('"');
                        if (q < 0)
                        {
                            src.CopyTo(res);
                            break;
                        }
                        src[..q].CopyTo(res);
                        if (q == src.Length || src[q + 1] != '"')
                        {
                            throw new FormatException("Unexpected single quote");
                        }
                        src = src[(q + 2)..];
                    }
                }).AsMemory();
            }
            return _raw.ToString();
        }

        public object ToType(Type conversionType, IFormatProvider? provider)
        {
            if (conversionType == typeof(byte[]))
            {
                var span = _raw.Span;
                if (span.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
                {
                    return Convert.FromHexString(span[2..]);
                }
            }

            throw new InvalidCastException();
        }
    }
}
