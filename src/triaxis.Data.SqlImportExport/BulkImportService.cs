using System.Data;

namespace triaxis.Data.SqlImportExport;

public class BulkImportService(
    ILogger<BulkImportService> logger
) : IBulkImportService
{
    public async Task BulkImportAsync(SqlConnection sqlConnection, IAsyncEnumerable<IBulkImportSource> input, BulkImportOptions? options = null)
    {
        if (sqlConnection.State != ConnectionState.Open)
        {
            logger.LogDebug("Opening connection for bulk import");
            // auto open the connection for the duration of the call
            await sqlConnection.OpenAsync();

            try
            {
                await BulkImportAsync(sqlConnection, input, options);
                return;
            }
            finally
            {
                logger.LogDebug("Closing connection after bulk import");
                await sqlConnection.CloseAsync();
            }
        }

        int batchSize = options?.BatchSize ?? BulkImportOptions.DefaultBatchSize;

        await using var transaction = await sqlConnection.BeginTransactionAsync();

        using var bcp = new SqlBulkCopy(sqlConnection,
            SqlBulkCopyOptions.KeepIdentity | SqlBulkCopyOptions.KeepNulls, (SqlTransaction)transaction)
        {
            BulkCopyTimeout = (int)(options?.Timeout ?? BulkImportOptions.DefaultTimeout).TotalSeconds,
            EnableStreaming = true,
            BatchSize = batchSize,
            NotifyAfter = batchSize,
        };

        await foreach (var source in input)
        {
            if (options?.Truncate == true)
            {
                logger.LogWarning("Replacing data in {TableName}", source.Name);
                await sqlConnection.ExecuteAsync($"TRUNCATE TABLE {source.Name}");
            }
            else
            {
                logger.LogDebug("Importing data into {TableName}", source.Name);
            }

            var fields = await source.GetColumnNamesAsync();
            await using var reader = source.EnumerateDataAsync().GetAsyncEnumerator();
            using var dataSource = new DataReader(fields, reader);
            bcp.DestinationTableName = source.Name;
            bcp.ColumnMappings.Clear();

            for (int i = 0; i < dataSource.Fields.Length; i++)
            {
                bcp.ColumnMappings.Add(i, dataSource.Fields[i]);
            }

            await bcp.WriteToServerAsync(dataSource);
        }

        await transaction.CommitAsync();
    }

    private class DataReader : IDataReader
    {
        private readonly string[] _fields;
        private IAsyncEnumerator<object[]>? _data;
        private object[] _values = null!;

        public DataReader(IEnumerable<string> fields, IAsyncEnumerator<object[]> data)
        {
            _fields = fields.ToArray();
            _data = data;
        }

        public object this[int i] => _values[i];

        public object this[string name] => throw new NotImplementedException();

        public int Depth => 0;
        public bool IsClosed => _data == null;
        public int RecordsAffected => 0;
        public int FieldCount => _fields.Length;
        public string[] Fields => _fields;

        public void Close() { _data = null; }
        public void Dispose() { Close(); }

        public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
            => throw new NotImplementedException();
        public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
            => throw new NotImplementedException();

        public IDataReader GetData(int i) => throw new NotImplementedException();
        public string GetDataTypeName(int i) => throw new NotImplementedException();
        public DataTable? GetSchemaTable() => throw new NotImplementedException();

        public bool GetBoolean(int i) => Convert.ToBoolean(_values[i]);
        public byte GetByte(int i) => Convert.ToByte(_values[i]);
        public char GetChar(int i) => Convert.ToChar(_values[i]);
        public DateTime GetDateTime(int i) => Convert.ToDateTime(_values[i]);
        public decimal GetDecimal(int i) => Convert.ToDecimal(_values[i]);
        public double GetDouble(int i) => Convert.ToDouble(_values[i]);
        public Type GetFieldType(int i) => _values[i]?.GetType() ?? DBNull.Value.GetType();
        public float GetFloat(int i) => Convert.ToSingle(_values[i]);
        public Guid GetGuid(int i) => (Guid)_values[i];
        public short GetInt16(int i) => Convert.ToInt16(_values[i]);
        public int GetInt32(int i) => Convert.ToInt32(_values[i]);
        public long GetInt64(int i) => Convert.ToInt64(_values[i]);
        public string GetString(int i) => _values[i].ToString() ?? "";
        public object GetValue(int i) => _values[i];

        public int GetValues(object[] values)
        {
            _values.CopyTo(values, 0);
            return _values.Length;
        }

        public bool IsDBNull(int i) => _values[i] == DBNull.Value;

        public string GetName(int i) => _fields[i];
        public int GetOrdinal(string name)
        {
            for (int i = 0; i < _fields.Length; i++)
            {
                if (_fields[i].Equals(name, StringComparison.OrdinalIgnoreCase))
                {
                    return i;
                }
            }
            return -1;
        }

        public bool NextResult() => false;

        public bool Read()
        {
            if (_data == null)
            {
                return false;
            }

            // TODO: maybe implement DbDataReader which has async overloads for Read as well
            if (!_data.MoveNextAsync().AsTask().ConfigureAwait(false).GetAwaiter().GetResult())
            {
                Close();
                return false;
            }

            _values = _data.Current;
            return true;
        }
    }
}
