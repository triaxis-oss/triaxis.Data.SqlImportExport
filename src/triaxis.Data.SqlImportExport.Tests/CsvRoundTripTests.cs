namespace triaxis.Data.SqlImportExport.Tests;

public class CsvRoundTripTests
{
    [Test]
    public async Task NullFirstField_KeepsColumnAlignment()
    {
        // a NULL in the first column used to swallow the following separator, shifting the
        // whole row left - and the import parsed the shifted row without any error
        var text = new StringWriter();
        await using (var writer = new CsvWriter(text, ["A", "B", "C"]))
        {
            await writer.WriteRecordAsync([null, "x", "y"]);
            await writer.WriteRecordAsync([null, null, "z"]);
        }

        Assert.That(text.ToString(), Is.EqualTo("A,B,C\r\n,x,y\r\n,,z\r\n".ReplaceLineEndings(Environment.NewLine)));

        var rows = new List<object?[]>();
        await foreach (var row in new CsvSource("T", new StringReader(text.ToString())).EnumerateDataAsync())
        {
            rows.Add(row);
        }

        Assert.That(rows[0].Select(Render), Is.EqualTo(new[] { null, "x", "y" }));
        Assert.That(rows[1].Select(Render), Is.EqualTo(new[] { null, null, "z" }));
    }

    private static string? Render(object? cell) => cell is null or DBNull ? null : cell.ToString();
}
